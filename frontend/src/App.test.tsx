import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { App } from "./App";
import { jobFixture } from "./test/fixtures";

const mocks = vi.hoisted(() => ({ useKleinJob: vi.fn() }));

vi.mock("./pages/klein/hook/useKleinJobs", () => ({
  useKleinJob: mocks.useKleinJob,
}));
vi.mock("./pages/klein/KleinJobsPage", () => ({
  KleinJobsPage: () => <div>Klein jobs screen</div>,
}));
vi.mock("./pages/klein/KleinJobOverviewPage", () => ({
  KleinJobOverviewPage: () => <div>Job overview screen</div>,
}));
vi.mock("./pages/klein/KleinCheckpointsPage", () => ({
  KleinCheckpointsPage: () => <div>Checkpoints screen</div>,
}));
vi.mock("./pages/klein/KleinConfigurationPage", () => ({
  KleinConfigurationPage: () => <div>Configuration screen</div>,
}));

beforeEach(() => {
  mocks.useKleinJob.mockReset();
  mocks.useKleinJob.mockReturnValue({ job: jobFixture() });
  window.location.hash = "";
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue({
      json: vi.fn().mockResolvedValue({
        ray_dashboard_url: "https://ray.example.test/",
      }),
    }),
  );
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

it("redirects the root route and builds Ray navigation from dashboard config", async () => {
  render(<App />);

  expect(await screen.findByText("Klein jobs screen")).toBeInTheDocument();
  expect(window.location.hash).toBe("#/klein");
  await waitFor(() =>
    expect(screen.getByRole("link", { name: "Open Ray Dashboard" })).toHaveAttribute(
      "href",
      "https://ray.example.test/#/",
    ),
  );
  expect(screen.getByRole("link", { name: "Jobs" })).toHaveAttribute(
    "href",
    "https://ray.example.test/#/jobs",
  );
});

it("renders nested job routes with breadcrumbs and side navigation", async () => {
  window.location.hash = "#/klein/jobs/job-1/checkpoints";

  render(<App />);

  expect(await screen.findByText("Checkpoints screen")).toBeInTheDocument();
  expect(screen.getByText("Orders")).toBeInTheDocument();
  expect(mocks.useKleinJob).toHaveBeenCalledWith("job-1");
  expect(screen.getByLabelText("Overview")).toHaveAttribute(
    "href",
    "#/klein/jobs/job-1",
  );
  expect(screen.getByRole("link", { name: "Checkpoints" })).toHaveAttribute(
    "href",
    "#/klein/jobs/job-1/checkpoints",
  );
});
