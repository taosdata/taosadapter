package wrapper

import (
	"net"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
)

func TestWhitelistCallback_ErrorCode(t *testing.T) {
	// Create a channel to receive the result
	resultChan := make(chan *WhitelistResult, 1)
	handle := cgo.NewHandle(resultChan)
	// Simulate an error (code != 0)
	go WhitelistCallback(handle.Pointer(), 1, nil, 0, nil)

	// Expect the result to have an error code
	result := <-resultChan
	assert.NotNil(t, result.Err)
	assert.Equal(t, int32(1), result.Err.(*errors.TaosError).Code)
	assert.Nil(t, result.IPNets) // No IPs should be returned
}

func TestWhitelistCallback_Success(t *testing.T) {
	// Prepare the test data: a list of byte slices representing IPs and masks
	ipList := []string{
		"192.168.1.1/24",
		"10.0.0.1/16",
		"2001:db8::/32",
	}
	addrList := make([]unsafe.Pointer, len(ipList))
	for i := 0; i < len(ipList); i++ {
		bs := make([]byte, len(ipList[i])+1)
		copy(bs, ipList[i])
		addrList[i] = unsafe.Pointer(&bs[0])
	}

	// Create a channel to receive the result
	resultChan := make(chan *WhitelistResult, 1)

	// Cast the byte slice to an unsafe pointer
	pWhiteLists := unsafe.Pointer(&addrList[0])
	handle := cgo.NewHandle(resultChan)
	// Simulate a successful callback (code == 0)
	go WhitelistCallback(handle.Pointer(), 0, nil, 3, pWhiteLists)

	// Expect the result to have two IPNets
	result := <-resultChan
	assert.Nil(t, result.Err)
	assert.Len(t, result.IPNets, 3)

	// Validate the first IPNet (192.168.1.1/24)
	assert.Equal(t, net.IPv4(192, 168, 1, 0).To4(), result.IPNets[0].IP)

	ones, _ := result.IPNets[0].Mask.Size()
	assert.Equal(t, 24, ones)

	// Validate the second IPNet (10.0.0.1/16)
	assert.Equal(t, net.IPv4(10, 0, 0, 0).To4(), result.IPNets[1].IP)
	ones, _ = result.IPNets[1].Mask.Size()
	assert.Equal(t, 16, ones)

	// Validate the third IPNet (2001:db8::/32)
	assert.Equal(t, net.ParseIP("2001:db8::"), result.IPNets[2].IP)
	ones, _ = result.IPNets[2].Mask.Size()
	assert.Equal(t, 32, ones)
}
