# The referenced CloudFormation stack was deleted in AWS on 2026-08-31.
# Forget its stale state entry on the next approved apply; never delete a live stack.
removed {
  from = aws_cloudformation_stack.mwaa_workflow

  lifecycle {
    destroy = false
  }
}
