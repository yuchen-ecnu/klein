# SPDX-License-Identifier: Apache-2.0
"""Control-plane errors for deployment, placement, and teardown."""

from ray.klein.exceptions import KleinError


class ControlPlaneError(KleinError):
    """Base for any failure in the job control plane (deploy/placement/stop)."""


class DeploymentError(ControlPlaneError):
    """A named deploy stage failed.

    Carries the stage name so the single catch site in ``schedule()`` can report
    *which* stage failed without each stage formatting its own message.
    """

    def __init__(self, stage: str, cause: object) -> None:
        self.stage = stage
        self.cause = cause
        super().__init__(f"deploy stage '{stage}' failed: {cause}")


class PlacementError(DeploymentError):
    """No feasible placement could be computed by a placement strategy."""

    def __init__(self, strategy: str, cause: object) -> None:
        self.strategy = strategy
        super().__init__("create workers", f"placement '{strategy}' infeasible: {cause}")


class CoordinatorError(ControlPlaneError):
    """The checkpoint coordinator could not be opened, started, or recovered."""


class TeardownError(ControlPlaneError):
    """Worker teardown could not reconcile (e.g. a survivor could not be killed)."""
