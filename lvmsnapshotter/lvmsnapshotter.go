/*
Copyright Intel Corporation

Fill this thing up
*/

package lvmsnapshotter

import (
	"context"
	"fmt"
	"io/ioutil"
	llog "log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/containerd/snapshots/storage"

	"github.com/containerd/continuity/fs"
	"github.com/pkg/errors"
)

var vImgSize = "10G"
var metaVolumeMountPath = "/mnt/contd-lvm-snapshotter-db-holder/"
var metavolume = "contd-metadata-holder"

type snapshotter struct {
	vgname     string
	lvpoolname string
	ms         *storage.MetaStore
}

// NewSnapshotter returns a Snapshotter which copies layers on the underlying
// file system. A metadata file is stored under the root.
func NewSnapshotter(vgname string, lvpoolname string) (snapshots.Snapshotter, error) {
	llog.Printf("Starting a new lvm snapshotter\n")

	if vgname == "" || lvpoolname == "" {
		llog.Printf("Either vgname or lvpoolname is empty. Both are needed for the plugin to work\n")
		return nil, os.ErrNotExist
	}

	// Check if the mount directory exists. If not, create it. if it is a file then exit

	if _, err := checkVG(vgname); err != nil {
		return nil, errors.Wrap(err, "VG not found")
	}

	_, err := checkLV(vgname, lvpoolname)
	if err != nil {
		return nil, errors.Wrap(err, "LV not found")
	}

	_, err = checkLV(vgname, metavolume)
	if err != nil {
		// Create a volume to hold the metadata.db file.
		if _, err := createLVMVolume(metavolume, vgname, lvpoolname, "", snapshots.KindUnknown); err != nil {
			llog.Printf("Unable to create the metadata volume\n")
			return nil, err
		}
	} else {
		llog.Printf("Re-using the existing volume\n")
	}
	if _, err := toggleactivateLV(vgname, metavolume, true); err != nil {
		return nil, errors.Wrap(err, "Unable to activate metavolume")
	}
	if fi, err := os.Stat(metaVolumeMountPath); os.IsNotExist(err) {
		if errdir := os.MkdirAll(metaVolumeMountPath, 0700); errdir != nil {
			return nil, err
		}
	} else {
		if !fi.IsDir() {
			return nil, err
		}
	}

	cmd := "mount"
	args := []string{"-rw", "/dev/" + vgname + "/" + metavolume, metaVolumeMountPath}
	if _, err := runCommand(cmd, args); err != nil {
		return nil, err
	}
	ms, err := storage.NewMetaStore(filepath.Join(metaVolumeMountPath, "metadata.db"))
	if err != nil {
		return nil, err
	}

	return &snapshotter{
		vgname:     vgname,
		lvpoolname: lvpoolname,
		ms:         ms,
	}, nil
}

// Stat returns the info for an active or committed snapshot by name or
// key.
//
// Should be used for parent resolution, existence checks and to discern
// the kind of snapshot.
func (o *snapshotter) Stat(ctx context.Context, key string) (snapshots.Info, error) {
	llog.Printf("Stat called for key %s\n", key)
	ctx, t, err := o.ms.TransactionContext(ctx, false)
	if err != nil {
		return snapshots.Info{}, err
	}
	defer t.Rollback()
	_, info, _, err := storage.GetInfo(ctx, key)
	if err != nil {
		return snapshots.Info{}, err
	}

	return info, nil
}

func (o *snapshotter) Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (snapshots.Info, error) {
	llog.Printf("Updating snapshot with info  %+v\n", info)
	ctx, t, err := o.ms.TransactionContext(ctx, true)
	if err != nil {
		return snapshots.Info{}, err
	}

	info, err = storage.UpdateInfo(ctx, info, fieldpaths...)
	if err != nil {
		t.Rollback()
		return snapshots.Info{}, err
	}

	if err := t.Commit(); err != nil {
		return snapshots.Info{}, err
	}

	return info, nil
}

func (o *snapshotter) Usage(ctx context.Context, key string) (snapshots.Usage, error) {
	llog.Printf("Finding usage of key  %+v\n", key)
	ctx, t, err := o.ms.TransactionContext(ctx, false)
	var mountpath string
	if err != nil {
		return snapshots.Usage{}, err
	}
	defer t.Rollback()

	id, info, usage, err := storage.GetInfo(ctx, key)
	if err != nil {
		return snapshots.Usage{}, err
	}

	if info.Kind == snapshots.KindActive {
		if mountpath, err = mountVolume(o.vgname, id); err != nil {
			return snapshots.Usage{}, errors.Wrap(err, "Unable to mount volume to calculate usage")
		}
		defer unmountVolume(mountpath)
		du, err := fs.DiskUsage(ctx, o.getSnapshotDir(id))
		if err != nil {
			return snapshots.Usage{}, err
		}
		usage = snapshots.Usage(du)
	}

	llog.Printf("usage of key %s is %+v\n", key, usage)
	return usage, nil
}

func (o *snapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	llog.Printf("Starting prepare\n")
	return o.createSnapshot(ctx, snapshots.KindActive, key, parent, opts)
}

func (o *snapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	llog.Printf("Starting View\n")
	return o.createSnapshot(ctx, snapshots.KindView, key, parent, opts)
}

// Mounts returns the mounts for the transaction identified by key. Can be
// called on an read-write or readonly transaction.
//
// This can be used to recover mounts after calling View or Prepare.
func (o *snapshotter) Mounts(ctx context.Context, key string) ([]mount.Mount, error) {
	llog.Printf("Finding mounts for key %s\n", key)
	ctx, t, err := o.ms.TransactionContext(ctx, false)
	if err != nil {
		return nil, err
	}
	s, err := storage.GetSnapshot(ctx, key)
	t.Rollback()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get snapshot mount")
	}
	llog.Printf("Mounts for key %s is %+v\n", key, o.mounts(s))
	return o.mounts(s), nil
}

func (o *snapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error {
	llog.Printf("Committing snapshot for key %s\n", key)
	ctx, t, err := o.ms.TransactionContext(ctx, true)
	if err != nil {
		return err
	}

	id, _, _, err := storage.GetInfo(ctx, key)
	if err != nil {
		return err
	}

	if _, err := storage.CommitActive(ctx, key, name, snapshots.Usage{}, opts...); err != nil {
		if rerr := t.Rollback(); rerr != nil {
			log.G(ctx).WithError(rerr).Warn("failed to rollback transaction")
		}
		return errors.Wrap(err, "failed to commit snapshot")
	}

	err = t.Commit()
	if err != nil {
		llog.Printf("Commit of transaction %+v failed\n", t)
		if _, derr := removeLVMVolume(o.vgname, id); derr != nil {
			llog.Printf("Unable to delete volume %s", id)
		}
		return err
	}
	return nil
}

// Remove abandons the transaction identified by key. All resources
// associated with the key will be removed.
func (o *snapshotter) Remove(ctx context.Context, key string) (err error) {
	llog.Printf("Remove contents of key %s\n", key)
	ctx, t, err := o.ms.TransactionContext(ctx, true)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil && t != nil {
			if rerr := t.Rollback(); rerr != nil {
				log.G(ctx).WithError(rerr).Warn("failed to rollback transaction")
			}
		}
	}()

	id, _, err := storage.Remove(ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to remove")
	}

	_, err = removeLVMVolume(id, o.vgname)
	if err != nil {
		return errors.Wrap(err, "failed to deletel LVM volume")
	}

	err = t.Commit()
	t = nil
	if err != nil {
		return errors.Wrap(err, "failed to commit")
	}

	return nil
}

// Walk the committed snapshots.
func (o *snapshotter) Walk(ctx context.Context, fn func(context.Context, snapshots.Info) error) error {
	llog.Printf("Walking through %+v\n", ctx)
	ctx, t, err := o.ms.TransactionContext(ctx, false)
	if err != nil {
		return err
	}
	defer t.Rollback()
	return storage.WalkInfo(ctx, fn)
}

func (o *snapshotter) createSnapshot(ctx context.Context, kind snapshots.Kind, key, parent string, opts []snapshots.Opt) (_ []mount.Mount, err error) {

	pvol := ""
	ctx, t, err := o.ms.TransactionContext(ctx, true)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil && t != nil {
			if rerr := t.Rollback(); err != nil {
				log.G(ctx).WithError(rerr).Warn("Failed to rollback transaction")
			}
		}
	}()

	s, err := storage.CreateSnapshot(ctx, kind, key, parent, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create snapshot")
	}

	if len(s.ParentIDs) == 0 {
		// Create a new logical volume without a base snapshot
		pvol = ""
	} else {
		// Create a snapshot from the parent
		pvol = s.ParentIDs[0]
	}
	if _, err := createLVMVolume(s.ID, o.vgname, o.lvpoolname, pvol, kind); err != nil {
		llog.Printf("Unable to create volume")
		return nil, errors.Wrap(err, "Unable to create volume")
	}

	if err := t.Commit(); err != nil {
		return nil, err
	}

	llog.Printf("Mounts for key in func createsnapshot %s is %+v\n", key, o.mounts(s))
	return o.mounts(s), nil
}

func (o *snapshotter) getSnapshotDir(id string) string {
	return filepath.Join("/dev", o.vgname, id)
}

func (o *snapshotter) mounts(s storage.Snapshot) []mount.Mount {
	var (
		roFlag string
		source string
	)

	if s.Kind == snapshots.KindView {
		roFlag = "-oro"
	}

	source = o.getSnapshotDir(s.ID)
	return []mount.Mount{
		{
			Source: source,
			Type:   "ext4",
			Options: []string{
				roFlag,
			},
		},
	}
}

// Close closes the snapshotter
func (o *snapshotter) Close() error {
	llog.Printf("Close function called")
	var err error
	err = o.ms.Close()
	if err != nil {
		return err
	}
	return unmountVolume(metaVolumeMountPath)
}

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
	if out, err := runCommand(cmd, args); err != nil {
		return out, errors.Wrap(err, "Unable to create volume")
	}

	if out, err = toggleactivateLV(vgname, lvname, true); err != nil {
		return out, errors.Wrap(err, "Unable to activate thin volume")
	}

	if parent == "" {
		//This volume is fresh. We should format it.
		cmd = "mkfs.ext4"
		args = []string{"-E", "nodiscard,lazy_itable_init=0,lazy_journal_init=0", "/dev/" + vgname + "/" + lvname}
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
	output := ""
	cmd := "vgs"
	args := []string{vgname, "--options", "vg_name", "--no-headings"}
	if output, err := runCommand(cmd, args); err != nil {
		llog.Printf("VG %s not found", vgname)
		return output, err
	}
	return output, nil
}

func checkLV(vgname string, lvname string) (string, error) {
	output := ""
	cmd := "lvs"
	args := []string{vgname + "/" + lvname, "--options", "lv_name", "--no-heading"}
	if output, err := runCommand(cmd, args); err != nil {
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
	args := []string{"-oro", "-t", "ext4", "/dev/" + vgname + "/" + lvname}
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
