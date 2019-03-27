package lvmsnapshotter

import (
	"github.com/docker/go-units"
	"github.com/pkg/errors"
)

const (
	defaultImgSize  = "10G"
	defaultFsType   = "xfs"
	defaultRootPath = "/mnt"
)

type LVMSnapConfig struct {
	// Root directory of snapshotter
	RootPath string `toml:"root_path"`

	// Volume group that will hold all the thin volumes
	VgName string `toml:"vol_group"`

	// Logical volume thin pool to hold all the volumes
	ThinPool string `toml:"thin_pool"`

	// Characteristics of the volumes that we will create
	ImageSize string `toml:"img_size"`
	FsType    string `toml:"fs_type"`
}

func (c *LVMSnapConfig) Validate(crootpath string) error {
	if c.VgName == "" || c.ThinPool == "" {
		return errors.New("Need both vol_group and thin_pool to be set")
	}

	if c.RootPath == "" {
		if crootpath != "" {
			c.RootPath = crootpath
		} else {
			c.RootPath = defaultRootPath
		}
	}

	if c.ImageSize == "" {
		c.ImageSize = defaultImgSize
	} else {
		// make sure it is a consumable value
		if val, err := units.FromHumanSize(c.ImageSize); err != nil {
			return err
		} else {
			c.ImageSize = units.HumanSize(float64(val))
		}
	}

	if c.FsType == "" {
		c.FsType = defaultFsType
	}
	return nil
}
