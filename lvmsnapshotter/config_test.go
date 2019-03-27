package lvmsnapshotter

import (
	"gotest.tools/assert"
	"testing"
)

const (
	rootpath = "/tmp/test_root"
)

func TestValidateConfig(t *testing.T) {

	c := LVMSnapConfig{}
	err := c.Validate("")
	assert.Error(t, err, "Need both vol_group and thin_pool to be set")

	expected := LVMSnapConfig{
		VgName:    "test_vg",
		ThinPool:  "test_pool",
		ImageSize: "10G",
		FsType:    "xfs",
		RootPath:  "/mnt",
	}

	c.VgName = "test_vg"
	c.ThinPool = "test_pool"
	err = c.Validate("")
	assert.NilError(t, err)
	assert.Equal(t, c, expected)

	c = LVMSnapConfig{
		VgName:   "test_vg",
		ThinPool: "test_pool",
	}

	expected = LVMSnapConfig{
		VgName:    "test_vg",
		ThinPool:  "test_pool",
		ImageSize: "10G",
		FsType:    "xfs",
		RootPath:  rootpath,
	}

	err = c.Validate(rootpath)
	assert.NilError(t, err)
	assert.Equal(t, c, expected)
}
