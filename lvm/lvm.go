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
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/containerd/containerd/snapshots"
	"github.com/pkg/errors"
)

const retries = 10

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

func createVolumeGroup(drive string, vgname string) (string, error) {
	cmd := "vgcreate"
	args := []string{vgname, drive}

	return runCommand(cmd, args)
}

func createLogicalThinPool(vgname string, lvpool string) (string, error) {
	cmd := "lvcreate"
	args := []string{"--thinpool", lvpool, "--extents", "90%FREE", vgname}

	fmt.Println(args)
	out, err := runCommand(cmd, args)
	if err != nil && (err.Error() == "exit status 5") {
		return out, nil
	}
	return out, err
}

func deleteVolumeGroup(vgname string) (string, error) {
	cmd := "vgremove"
	args := []string{"-y", vgname}

	return runCommand(cmd, args)
}

func checkVG(vgname string) (string, error) {
	var err error
	output := ""
	cmd := "vgs"
	args := []string{vgname, "--options", "vg_name", "--no-headings"}
	output, err = runCommand(cmd, args)
	return output, err
}

func checkLV(vgname string, lvname string) (string, error) {
	var err error
	output := ""
	cmd := "lvs"
	args := []string{vgname + "/" + lvname, "--options", "lv_name", "--no-heading"}
	output, err = runCommand(cmd, args)
	return output, err
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
	output := ""
	var err error

	if activate {
		args = append(args, "y")
	} else {
		args = append(args, "n")
	}
	output, err = runCommand(cmd, args)
	return output, err
}

func toggleactivateVG(vgname string, activate bool) (string, error) {
	cmd := "vgchange"
	args := []string{"-K", vgname, "-a"}
	output := ""
	var err error

	if activate {
		args = append(args, "y")
	} else {
		args = append(args, "n")
	}
	output, err = runCommand(cmd, args)
	return output, err
}

func runCommand(cmd string, args []string) (string, error) {
	var output []byte
	ret := 0
	var err error

	// Pass context down and log into the tool instead of this.
	fmt.Printf("Running command %s with args: %s\n", cmd, args)
	for ret < retries {
		c := exec.Command(cmd, args...)
		c.Env = os.Environ()
		c.SysProcAttr = &syscall.SysProcAttr{
			Pdeathsig: syscall.SIGTERM,
			Setpgid:   true,
		}

		output, err = c.CombinedOutput()
		if err == nil {
			break
		}
		ret++
		time.Sleep(100000 * time.Nanosecond)
	}

	return strings.TrimSpace(string(output)), err
}
