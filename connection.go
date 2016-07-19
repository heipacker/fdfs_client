package fdfs_client

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/laohanlinux/go-logger/logger"
)

var ErrClosed = errors.New("pool is closed")

type pConn struct {
	net.Conn
	pool *ConnectionPool
}

// not recycle the connection
func (c pConn) Close() error {
	return c.pool.put(c.Conn)
}

type ConnectionPool struct {
	hosts      []string
	ports      []int
	minConns   int
	maxConns   int
	busyConns  []bool
	conns      chan net.Conn
	trackerIdx int
	idxLock    sync.Locker
}

func minInt(a int, b int) int {
	if b-a > 0 {
		return a
	} else {
		return b
	}
}

func NewConnectionPool(hosts []string, ports []int, minConns int, maxConns int) (*ConnectionPool, error) {
	if minConns < 0 || maxConns <= 0 || minConns > maxConns {
		err := errors.New("invalid conns settings")
		logger.Error(err)
		return nil, err
	}
	cp := &ConnectionPool{
		hosts:      hosts,
		ports:      ports,
		minConns:   minConns,
		maxConns:   maxConns,
		conns:      make(chan net.Conn, maxConns),
		busyConns:  make([]bool, len(hosts)),
		trackerIdx: 0,
		idxLock:    &sync.Mutex{},
	}
	for i := 0; i < minInt(MINCONN, len(hosts)); i++ {
		conn, err := cp.makeConn()
		if err != nil {
			logger.Fatal("make connection error", err.Error())
			break
		}
		cp.conns <- conn
	}
	// set rand time
	return cp, nil
}

// Get return a tracker activeConn if successfauly.
func (this *ConnectionPool) Get() (net.Conn, error) {
	conns := this.getConns()
	if conns == nil {
		logger.Fatal(this.conns)
		//return nil, ErrClosed
	}

	tryTime := this.maxConns
	for {
		select {
		case conn := <-conns:
			if conn == nil {
				break
			}
			if err := this.activeConn(conn); err != nil {
				logger.Error("check the connection alive error:", conn.RemoteAddr(), err)
				break
			}
			logger.Debug("tracker connection is:", conn.RemoteAddr().String(), " at this time")
			return this.wrapConn(conn), nil
		default:

			if this.Len() >= this.maxConns {
				errmsg := fmt.Sprintf("Too many connctions %d", this.Len())
				return nil, errors.New(errmsg)
			}
			conn, err := this.makeConn()
			if err != nil && tryTime > 0 {
				logger.Error("can not get activeConn:", err, conn)
				break
			}
			if err := this.activeConn(conn); err != nil && tryTime > 0 {
				break
			}
			if tryTime == 0 {
				return nil, errors.New("can not get the activeConn")
			}
			this.conns <- conn
			//put connection to pool and go next `for` loop
			//return this.wrapConn(conn), nil
		}
		tryTime--
	}
}

func (this *ConnectionPool) Close() {
	conns := this.conns
	this.conns = nil

	if conns == nil {
		return
	}

	close(conns)

	for conn := range conns {
		conn.Close()
	}
}

func (this *ConnectionPool) Len() int {
	return len(this.getConns())
}

// use robin type
/*
func (this *ConnectionPool) makeConn() (net.Conn, error) {
	this.idxLock.Lock()
	idx := int(this.trackerIdx)
	addr := fmt.Sprintf("%s:%d", this.hosts[idx], this.ports[idx])
	this.trackerIdx++
	if this.trackerIdx >= len(this.hosts) {
		this.trackerIdx = 0
	}
	this.idxLock.Unlock()

	c, err := net.DialTimeout("tcp", addr, time.Second*10)
	if err != nil {
		return c, err
	}
	c.SetDeadline(time.Now().Add(time.Duration(60) * time.Second))
	return c, err
}
*/
// use robin type
func (this *ConnectionPool) makeConn() (net.Conn, error) {

	for i := 0; i < len(this.hosts); i++ {
		addr := fmt.Sprintf("%s:%d", this.hosts[i], this.ports[i])
		c, err := net.DialTimeout("tcp", addr, time.Second*10)
		if err != nil {
			return c, err
		}
		c.SetDeadline(time.Now().Add(time.Duration(60) * time.Second))
		logger.Info("The tracker addr is: ", addr)
		return c, err
	}

	return nil, fmt.Errorf("all tracker is dead")
}

func (this *ConnectionPool) getConns() chan net.Conn {
	conns := this.conns
	return conns
}

func (this *ConnectionPool) put(conn net.Conn) error {
	if conn == nil {
		return errors.New("connection is nil")
	}
	if this.conns == nil {
		return conn.Close()
	}
	return conn.Close()

	//select {
	//case this.conns <- conn:
	//	return nil
	//default:
	//	return conn.Close()
	//}
}

func (this *ConnectionPool) wrapConn(conn net.Conn) net.Conn {
	c := pConn{pool: this}
	c.Conn = conn
	return c
}

func (this *ConnectionPool) activeConn(conn net.Conn) error {
	th := &trackerHeader{}
	th.cmd = FDFS_PROTO_CMD_ACTIVE_TEST
	th.sendHeader(conn)
	th.recvHeader(conn)
	if th.cmd == 100 && th.status == 0 {
		return nil
	}
	return errors.New("Conn unaliviable")
}

func TcpSendData(conn net.Conn, bytesStream []byte) error {
	if _, err := conn.Write(bytesStream); err != nil {
		return err
	}
	return nil
}

func TcpSendFile(conn net.Conn, filename string) error {
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		return err
	}

	var fileSize int64 = 0
	if fileInfo, err := file.Stat(); err == nil {
		fileSize = fileInfo.Size()
	}

	if fileSize == 0 {
		errmsg := fmt.Sprintf("file size is zeor [%s]", filename)
		return errors.New(errmsg)
	}

	fileBuffer := make([]byte, fileSize)

	_, err = file.Read(fileBuffer)
	if err != nil {
		return err
	}

	return TcpSendData(conn, fileBuffer)
}

func TcpRecvResponse(conn net.Conn, bufferSize int64) ([]byte, int64, error) {
	recvBuff := make([]byte, 0, bufferSize)
	tmp := make([]byte, 256)
	var total int64
	for {
		n, err := conn.Read(tmp)
		total += int64(n)
		recvBuff = append(recvBuff, tmp[:n]...)
		if err != nil {
			if err != io.EOF {
				return nil, 0, err
			}
			break
		}
		if total == bufferSize {
			break
		}
	}
	return recvBuff, total, nil
}

func TcpRecvFile(conn net.Conn, localFilename string, bufferSize int64) (int64, error) {
	file, err := os.Create(localFilename)
	defer file.Close()
	if err != nil {
		return 0, err
	}

	recvBuff, total, err := TcpRecvResponse(conn, bufferSize)
	if _, err := file.Write(recvBuff); err != nil {
		return 0, err
	}
	return total, nil
}
