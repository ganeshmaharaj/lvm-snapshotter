package lvm

import (
	"context"
	"fmt"
	"io/ioutil"
	"runtime"
	"testing"
	"os"
	"math/rand"

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
	sparseDrive  = "lvm-snapshot-test-*.img"
	letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

func randString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63() % int64(len(letterBytes))]
	}
	return string(b)
}

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

func createSparseDrive(t *testing.T, dir string) (string, string, error) {
	cwd, err := os.Getwd()
	assert.NilError(t, err)
	file, err := ioutil.TempFile(cwd, sparseDrive)
	assert.NilError(t, err)

	size, err := units.RAMInBytes("10Gb")
	assert.NilError(t, err)

	err = file.Truncate(size)
	assert.NilError(t, err)

	err = file.Close()
	assert.NilError(t, err)

	imagePath := file.Name()

	loopDevice, err := createLoopBackDevice(imagePath)
	return imagePath, loopDevice, err

}

func TestLVMSnapshotterSuite(t *testing.T) {
	var loopDevice, imagePath string
	var vgName string
	var lvPool string
	var err error
	if runtime.GOOS != "linux" {
		t.Skip("Snapshotter only implemented for Linux")
	}

	testutil.RequiresRoot(t)

	testLvmSnapshotter := func(ctx context.Context, root string) (snapshots.Snapshotter, func() error, error) {
		imagePath, loopDevice, err = createSparseDrive(t, root)
		assert.NilError(t, err)
		fmt.Printf("Using device %s", loopDevice)

		suffix := randString(10)

		vgName = vgNamePrefix + string(suffix)
		lvPool = lvPoolPrefix + string(suffix)

		output, err := createVolumeGroup(loopDevice, vgName)
		assert.NilError(t, err, output)

		output, err = createLogicalThinPool(vgName, lvPool)

		config := &SnapConfig{
			VgName:   vgName,
			ThinPool: lvPool,
		}
		err = config.Validate(root)
		assert.NilError(t, err)

		snap, err := NewSnapshotter(ctx, config)
		assert.NilError(t, err)

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
			if err := os.Remove(imagePath); err !=nil {
				return err
			}
			return nil
		}, nil
	}

	testsuite.SnapshotterSuite(t, "LVM", testLvmSnapshotter)

}
