package lvmsnapshotter

import (
	"fmt"
	"io/ioutil"
	llog "log"
	"os"
	"os/exec"
	"strings"

	"github.com/containerd/containerd/snapshots"
	"github.com/pkg/errors"
)

func createLVMVolume(lvname string, vgname string, lvpoolname string, parent string, kind snapshots.Kind) (string, error) {
	cmd := "lvcreate"
	args := []string{}
	out := ""
	var err error

	if parent != "" {
		args = append(args, "--name", lvname, "--snapshot", vgname+"/"+parent)
	} else {
		// Create a new logical volume without a base snapshot
		args = append(args, "--virtualsize", vImgSize, "--name", lvname, "--thin", vgname+"/"+lvpoolname)
	}

	if kind == snapshots.KindView {
		args = append(args, "-pr")
	}

	//Let's go and create the volume
	if out, err = runCommand(cmd, args); err != nil {
		return out, errors.Wrap(err, "Unable to create volume")
	}

	if out, err = toggleactivateLV(vgname, lvname, true); err != nil {
		return out, errors.Wrap(err, "Unable to activate thin volume")
	}

	if parent == "" {
		//This volume is fresh. We should format it.
		cmd = "mkfs.xfs"
		args = []string{"-f", "/dev/" + vgname + "/" + lvname}
		out, err = runCommand(cmd, args)
	}

	return out, err
}

func removeLVMVolume(lvname string, vgname string) (string, error) {
	cmd := "lvremove"
	args := []string{"-y", vgname + "/" + lvname}

	return runCommand(cmd, args)
}

func checkVG(vgname string) (string, error) {
	var err error
	output := ""
	cmd := "vgs"
	args := []string{vgname, "--options", "vg_name", "--no-headings"}
	if output, err = runCommand(cmd, args); err != nil {
		llog.Printf("VG %s not found", vgname)
		return output, err
	}
	return output, nil
}

func checkLV(vgname string, lvname string) (string, error) {
	var err error
	output := ""
	cmd := "lvs"
	args := []string{vgname + "/" + lvname, "--options", "lv_name", "--no-heading"}
	if output, err = runCommand(cmd, args); err != nil {
		llog.Printf("LV %s not found", lvname)
		return output, err
	}
	return output, nil
}

func changepermLV(vgname string, lvname string, readonly bool) (string, error) {
	cmd := "lvchange"
	args := []string{}

	if readonly {
		args = append(args, "-pr")
	} else {
		args = append(args, "-prw")
	}
	args = append(args, vgname+"/"+lvname)

	return runCommand(cmd, args)
}

func toggleactivateLV(vgname string, lvname string, activate bool) (string, error) {
	cmd := "lvchange"
	args := []string{"-K", vgname + "/" + lvname, "-a"}

	if activate {
		args = append(args, "y")
	} else {
		args = append(args, "n")
	}
	return runCommand(cmd, args)
}

func mountVolume(vgname string, lvname string) (string, error) {
	cmd := "mount"
	args := []string{"-oro", "-t", "xfs", "/dev/" + vgname + "/" + lvname}
	var mountPath string
	var err error

	if mountPath, err = ioutil.TempDir("", vgname+"-"+lvname); err != nil {
		return "", err
	}
	args = append(args, mountPath)

	if _, err := runCommand(cmd, args); err != nil {
		return "", err
	}
	return mountPath, nil
}

func unmountVolume(mountPath string) error {
	cmd := "umount"
	args := []string{mountPath}

	if _, err := runCommand(cmd, args); err != nil {
		return errors.Wrap(err, "Unable to unmount volume")
	}

	err := os.RemoveAll(mountPath)
	return err
}

func runCommand(cmd string, args []string) (string, error) {
	var output []byte

	fmt.Printf("Running command %s with args: %s\n", cmd, args)
	c := exec.Command(cmd, args...)
	output, err := c.CombinedOutput()
	return strings.TrimSpace(string(output)), err
}
