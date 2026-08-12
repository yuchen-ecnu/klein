import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, it, vi } from "vitest";
import { operatorFixture } from "../../test/fixtures";
import { KleinOperatorRescaleControl } from "./KleinOperatorRescaleControl";

const mocks = vi.hoisted(() => ({ useKleinOperatorRescale: vi.fn() }));

vi.mock("./hook/useKleinJobs", () => ({
  useKleinOperatorRescale: mocks.useKleinOperatorRescale,
}));

const clearFeedback = vi.fn();
const rescale = vi.fn();

beforeEach(() => {
  clearFeedback.mockReset();
  rescale.mockReset();
  rescale.mockResolvedValue(undefined);
  mocks.useKleinOperatorRescale.mockReturnValue({
    clearFeedback,
    isRescaling: false,
    operationPending: false,
    requestError: undefined,
    rescale,
    result: undefined,
  });
});

it("validates the target and confirms a scale-out operation", async () => {
  const user = userEvent.setup();
  render(
    <KleinOperatorRescaleControl
      jobId="job-1"
      onRefresh={vi.fn()}
      operator={operatorFixture({ parallelism: 2 })}
    />,
  );

  const input = screen.getByLabelText("Parallelism");
  await user.clear(input);
  expect(screen.getByText("Enter a positive integer.")).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Rescale" })).toBeDisabled();

  await user.type(input, "4");
  await user.click(screen.getByRole("button", { name: "Rescale" }));
  expect(screen.getByText("Rescale map?")).toBeInTheDocument();
  expect(screen.getByText(/creates only the added task instances/)).toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "Confirm rescale" }));

  await waitFor(() => expect(rescale).toHaveBeenCalledWith(1, 4));
});

it("explains why an operator cannot be rescaled", () => {
  render(
    <KleinOperatorRescaleControl
      jobId="job-1"
      onRefresh={vi.fn()}
      operator={operatorFixture({
        can_rescale: false,
        rescale_disabled_reason: "Transactional sink is active.",
      })}
    />,
  );

  expect(screen.getByText("Transactional sink is active.")).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Rescale" })).toBeDisabled();
});

it.each(["ACCEPTED", "RUNNING", "STABILIZING"] as const)(
  "shows and disables controls for a %s rescale operation",
  (status) => {
    mocks.useKleinOperatorRescale.mockReturnValue({
      clearFeedback,
      isRescaling: false,
      operationPending: true,
      requestError: undefined,
      rescale,
      result: {
        operation_id: "resize-1",
        job_id: "job-1",
        operator_id: 1,
        parallelism: 4,
        target_parallelism: 4,
        status,
        phase:
          status === "ACCEPTED"
            ? "QUEUED"
            : status === "RUNNING"
              ? "COORDINATING"
              : "STABILIZING",
      },
    });

    const { container } = render(
      <KleinOperatorRescaleControl
        jobId="job-1"
        onRefresh={vi.fn()}
        operator={operatorFixture({ parallelism: 2 })}
      />,
    );

    expect(
      within(container).getByText(
        new RegExp(`rescale is ${status.toLowerCase()}`),
      ),
    ).toBeInTheDocument();
    expect(
      within(container).getByRole("button", { name: "Rescaling…" }),
    ).toBeDisabled();
  },
);
