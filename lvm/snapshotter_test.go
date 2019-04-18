package lvm

import (
	"context"
	"fmt"
	"io/ioutil"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/containerd/containerd/pkg/testutil"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/containerd/snapshots/testsuite"
	units "github.com/docker/go-units"
	"github.com/pkg/errors"
	"gotest.tools/assert"
)

const (
	vgNamePrefix = "vgthin"
	lvPoolPrefix = "lvthinpool"
	sparseDrive  = "lvm-snapshot-test-"
)

func createLoopBackDevice(imgPath string) (string, error) {
	cmd := "losetup"
	args := []string{"--find", "--show", imgPath}

	return runCommand(cmd, args)
}

func deleteLoopBackDevice(loopDevice string) error {
	cmd := "losetup"
	args := []string{"--detach", loopDevice}

	_, err := runCommand(cmd, args)
	return err
}

func createSparseDrive(t *testing.T, dir string) (string, error) {
	file, err := ioutil.TempFile(dir, sparseDrive)
	assert.NilError(t, err)

	size, err := units.RAMInBytes("10Gb")
	assert.NilError(t, err)

	err = file.Truncate(size)
	assert.NilError(t, err)

	err = file.Close()
	assert.NilError(t, err)

	imagePath := file.Name()

	return createLoopBackDevice(imagePath)

}

func TestLVMSnapshotterSuite(t *testing.T) {
	var loopDevice string
	var vgName string
	var lvPool string
	var err error
	if runtime.GOOS != "linux" {
		t.Skip("Snapshotter only implemented for Linux")
	}

	testutil.RequiresRoot(t)

	testLvmSnapshotter := func(ctx context.Context, root string) (snapshots.Snapshotter, func() error, error) {
		loopDevice, err = createSparseDrive(t, root)
		assert.NilError(t, err)
		fmt.Printf("Using device %s", loopDevice)

		suffix := strconv.Itoa(time.Now().Nanosecond())

		vgName = vgNamePrefix + suffix
		lvPool = lvPoolPrefix + suffix

		output, err := createVolumeGroup(loopDevice, vgName)
		assert.NilError(t, err, output)

		output, err = createLogicalThinPool(vgName, lvPool)
		assert.NilError(t, err, output)

		config := &SnapConfig{
			VgName:   vgName,
			ThinPool: lvPool,
		}
		err = config.Validate(root)
		assert.NilError(t, err)

		snap, err := NewSnapshotter(ctx, config)
		assert.NilError(t, err)

		//testclose := func() error {
		//	fmt.Printf("Close called")
		//	snap.Close()
		//	deleteVolumeGroup(vgName)
		//	deleteLoopBackDevice(loopDevice)
		//	return nil
		//}

		return snap, func() error {
			if err := snap.Close(); err != nil {
				return err
			}
			if _, err := deleteVolumeGroup(vgName); err != nil {
				return errors.Wrap(err, "Unable to delete volume group")
			}
			if err := deleteLoopBackDevice(loopDevice); err != nil {
				return err
			}
			return nil
		}, nil
	}

	testsuite.SnapshotterSuite(t, "LVM", testLvmSnapshotter)

}
