import {
  cleanup,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { jobFixture } from "../../test/fixtures";
import { KleinJobOverviewPage } from "./KleinJobOverviewPage";

const mocks = vi.hoisted(() => ({ useKleinJob: vi.fn() }));

vi.mock("./hook/useKleinJobs", () => ({ useKleinJob: mocks.useKleinJob }));
vi.mock("./KleinJobGraph", () => ({
  KleinJobGraph: ({
    onOpenOperatorDetails,
  }: {
    onOpenOperatorDetails: (operatorId: number) => void;
  }) => (
    <button onClick={() => onOpenOperatorDetails(1)} type="button">
      Open graph operator
    </button>
  ),
}));
vi.mock("./KleinOperatorsTable", () => ({
  KleinOperatorsTable: () => <div>Operators table</div>,
}));
vi.mock("./KleinOperatorDetails", () => ({
  KleinOperatorDetails: ({ operator }: { operator?: { name: string } }) => (
    <div>{operator ? `Selected ${operator.name}` : "No selected operator"}</div>
  ),
}));

const cancel = vi.fn();
const refresh = vi.fn();

const renderPage = () =>
  render(
    <MemoryRouter initialEntries={["/jobs/job-1"]}>
      <Routes>
        <Route element={<KleinJobOverviewPage />} path="jobs/:jobId" />
      </Routes>
    </MemoryRouter>,
  );

beforeEach(() => {
  cancel.mockReset();
  refresh.mockReset();
  cancel.mockResolvedValue(undefined);
  mocks.useKleinJob.mockReset();
});

afterEach(cleanup);

it("renders loading and request failure states", () => {
  mocks.useKleinJob.mockReturnValue({
    cancel,
    error: undefined,
    isLoading: true,
    job: undefined,
    refresh,
  });
  const { rerender } = renderPage();
  expect(screen.getByRole("progressbar")).toBeInTheDocument();

  mocks.useKleinJob.mockReturnValue({
    cancel,
    error: new Error("offline"),
    isLoading: false,
    job: undefined,
    refresh,
  });
  rerender(
    <MemoryRouter initialEntries={["/jobs/job-1"]}>
      <Routes>
        <Route element={<KleinJobOverviewPage />} path="jobs/:jobId" />
      </Routes>
    </MemoryRouter>,
  );
  expect(
    screen.getByText(/Unable to load Klein job: Error: offline/),
  ).toBeInTheDocument();
});

it("renders health warnings, opens operator details, and cancels a running job", async () => {
  const user = userEvent.setup();
  mocks.useKleinJob.mockReturnValue({
    cancel,
    error: undefined,
    isLoading: false,
    job: jobFixture({
      dashboard_error: "manager timed out",
      dashboard_stale: true,
      failure: "worker failed",
    }),
    refresh,
  });
  renderPage();

  expect(screen.getByRole("heading", { name: "Orders" })).toBeInTheDocument();
  expect(screen.getByText(/manager timed out/)).toBeInTheDocument();
  expect(screen.getByText("worker failed")).toBeInTheDocument();
  expect(screen.getByText("10 / 9")).toBeInTheDocument();

  await user.click(screen.getByRole("button", { name: "Open graph operator" }));
  expect(screen.getByText("Selected map")).toBeInTheDocument();
  await user.click(
    screen.getByRole("button", { name: "Close operator details" }),
  );
  await waitFor(() =>
    expect(
      screen.queryByRole("button", { name: "Close operator details" }),
    ).not.toBeInTheDocument(),
  );

  await user.click(screen.getByRole("button", { name: "Cancel job" }));
  const dialog = screen.getByRole("dialog", { name: "Cancel Orders?" });
  await user.click(within(dialog).getByRole("button", { name: "Cancel job" }));
  await waitFor(() => expect(cancel).toHaveBeenCalledOnce());
  await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
});

it("reports a cancellation failure without hiding the confirmation", async () => {
  const user = userEvent.setup();
  cancel.mockRejectedValue(new Error("permission denied"));
  mocks.useKleinJob.mockReturnValue({
    cancel,
    error: undefined,
    isLoading: false,
    job: jobFixture(),
    refresh,
  });
  renderPage();

  await user.click(screen.getByRole("button", { name: "Cancel job" }));
  const dialog = screen.getByRole("dialog", { name: "Cancel Orders?" });
  await user.click(within(dialog).getByRole("button", { name: "Cancel job" }));

  expect(
    await screen.findByText("Unable to cancel job: Error: permission denied"),
  ).toBeInTheDocument();
  expect(dialog).toBeInTheDocument();
});
