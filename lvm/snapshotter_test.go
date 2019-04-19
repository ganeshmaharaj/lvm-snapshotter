package lvm

import (
	"context"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/containerd/containerd/pkg/testutil"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/containerd/snapshots/testsuite"
	"github.com/containerd/continuity/testutil/loopback"
	"gotest.tools/assert"
)

const (
	vgNamePrefix = "vgthin"
	lvPoolPrefix = "lvthinpool"
	loopbackSize = int64(10 << 30)
)

func TestLVMSnapshotterSuite(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Snapshotter only implemented for Linux")
	}

	testutil.RequiresRoot(t)

	testLvmSnapshotter := func(ctx context.Context, root string) (snapshots.Snapshotter, func() error, error) {
		var loopDevice *loopback.Loopback
		var vgName string
		var lvPool string
		var err error
		//imagePath, loopDevice, err = createSparseDrive(t, root)
		loopDevice, err = loopback.New(loopbackSize)
		assert.NilError(t, err)
		//fmt.Printf("Using device %s", loopDevice.Device)

		//suffix := randString(10)
		suffix := strconv.Itoa(time.Now().Nanosecond())

		vgName = vgNamePrefix + suffix
		lvPool = lvPoolPrefix + suffix

		output, err := createVolumeGroup(loopDevice.Device, vgName)
		assert.NilError(t, err, output)

		output, err = toggleactivateVG(vgName, true)
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

		return snap, func() error {
			defer func() {
				snap.Close()
				deleteVolumeGroup(vgName)
				loopDevice.Close()
			}()
			return nil
		}, nil
	}

	testsuite.SnapshotterSuite(t, "LVM", testLvmSnapshotter)

}
