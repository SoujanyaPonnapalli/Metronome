# Per-cell cluster placement group. DISABLED (count = 0) for the canonical
# E19 rerun: forcing N>=5 instances into one rack caused repeated
# InsufficientInstanceCapacity for parallel cells. Cell-to-cell isolation is
# already provided by separate instances + EBS + tfstate; a *cluster* PG only
# packs instances within a cell (negligible benefit for an fsync-bound
# workload). Resource block kept so servers.tf/driver.tf's
# try(aws_placement_group.cell[0].name, null) resolves to null. Set count
# back to `var.cross_az ? 0 : 1` to re-enable.
resource "aws_placement_group" "cell" {
  count    = 0
  name     = "metronome-eval-${var.cell_id}"
  strategy = "cluster"
  tags = merge(local.tags, {
    Name = "metronome-eval-${var.cell_id}-pg"
    Role = "placement-group"
  })
}
