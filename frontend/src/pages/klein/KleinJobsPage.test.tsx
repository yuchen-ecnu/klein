import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { beforeEach, expect, it, vi } from "vitest";
import { jobFixture } from "../../test/fixtures";
import { KleinJobsPage } from "./KleinJobsPage";

const mocks = vi.hoisted(() => ({ useKleinJobs: vi.fn() }));

vi.mock("./hook/useKleinJobs", () => ({ useKleinJobs: mocks.useKleinJobs }));

beforeEach(() => {
  mocks.useKleinJobs.mockReset();
});

it("renders job totals and an encoded details link", () => {
  mocks.useKleinJobs.mockReturnValue({
    jobs: [jobFixture({ job_id: "team/job 1" })],
    error: undefined,
    isLoading: false,
  });

  render(
    <MemoryRouter>
      <KleinJobsPage />
    </MemoryRouter>,
  );

  expect(screen.getByText("Orders")).toBeInTheDocument();
  expect(screen.getByText("2", { selector: "h5" })).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "Orders" })).toHaveAttribute(
    "href",
    "/jobs/team%2Fjob%201",
  );
});

it("renders loading, request failure, and empty states", () => {
  mocks.useKleinJobs.mockReturnValue({ jobs: [], error: undefined, isLoading: true });
  const { rerender } = render(
    <MemoryRouter>
      <KleinJobsPage />
    </MemoryRouter>,
  );
  expect(screen.getByRole("progressbar")).toBeInTheDocument();

  mocks.useKleinJobs.mockReturnValue({ jobs: [], error: new Error("offline"), isLoading: false });
  rerender(
    <MemoryRouter>
      <KleinJobsPage />
    </MemoryRouter>,
  );
  expect(screen.getByText(/Unable to load Klein jobs: Error: offline/)).toBeInTheDocument();
  expect(screen.getByText("No Klein jobs found")).toBeInTheDocument();
});
