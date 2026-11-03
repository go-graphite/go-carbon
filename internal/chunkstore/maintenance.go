package chunkstore

import "context"

func (s *Store) waitMaintenance(ctx context.Context, name string) error {
	if s.maintenanceWait != nil {
		return s.maintenanceWait(ctx, name)
	}
	return ctx.Err()
}
