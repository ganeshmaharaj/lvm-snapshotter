// +build linux

/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package lvm

import (
	"os"
	"os/exec"
	"strings"
	"syscall"

	"github.com/containerd/containerd/snapshots"
	"github.com/pkg/errors"
)

func createLVMVolume(lvname string, vgname string, lvpoolname string, size string, fstype string, parent string, kind snapshots.Kind) (string, error) {
	cmd := "lvcreate"
	args := []string{}
	out := ""
	var err error

	if parent != "" {
		args = append(args, "--name", lvname, "--snapshot", vgname+"/"+parent)
	} else {
		// Create a new logical volume without a base snapshot
		args = append(args, "--virtualsize", size, "--name", lvname, "--thin", vgname+"/"+lvpoolname)
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
		cmd = "mkfs." + fstype
		args = []string{"/dev/" + vgname + "/" + lvname}
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
		return output, errors.Wrapf(err, "VG %s not found", vgname)
	}
	return output, nil
}

func checkLV(vgname string, lvname string) (string, error) {
	var err error
	output := ""
	cmd := "lvs"
	args := []string{vgname + "/" + lvname, "--options", "lv_name", "--no-heading"}
	if output, err = runCommand(cmd, args); err != nil {
		return output, errors.Wrapf(err, "LV %s not found", lvname)
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

func runCommand(cmd string, args []string) (string, error) {
	var output []byte

	// Pass context down and log into the tool instead of this.
	//fmt.Printf("Running command %s with args: %s\n", cmd, args)
	c := exec.Command(cmd, args...)
	c.Env = os.Environ()
	c.SysProcAttr = &syscall.SysProcAttr{
		Pdeathsig: syscall.SIGTERM,
	}

	output, err := c.CombinedOutput()
	return strings.TrimSpace(string(output)), err
}
