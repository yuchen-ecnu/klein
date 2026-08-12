import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { beforeEach, expect, it, vi } from "vitest";
import { jobFixture } from "../../test/fixtures";
import { KleinConfigurationPage } from "./KleinConfigurationPage";

const mocks = vi.hoisted(() => ({ useKleinJob: vi.fn() }));

vi.mock("./hook/useKleinJobs", () => ({ useKleinJob: mocks.useKleinJob }));

beforeEach(() => mocks.useKleinJob.mockReset());

it("renders metadata and stringifies non-string options", () => {
  mocks.useKleinJob.mockReturnValue({
    job: jobFixture({ configuration: { retries: 3, mode: "streaming" } }),
    error: undefined,
    isLoading: false,
  });

  render(
    <MemoryRouter>
      <KleinConfigurationPage />
    </MemoryRouter>,
  );

  expect(screen.getAllByText("job-1")).toHaveLength(2);
  expect(screen.getByText("retries")).toBeInTheDocument();
  expect(screen.getByText("3")).toBeInTheDocument();
  expect(screen.getByText("streaming")).toBeInTheDocument();
  expect(screen.getByText(/Credential-like options are redacted/)).toBeInTheDocument();
});

it("renders loading and missing-job states", () => {
  mocks.useKleinJob.mockReturnValue({ job: undefined, error: undefined, isLoading: true });
  const { rerender } = render(
    <MemoryRouter>
      <KleinConfigurationPage />
    </MemoryRouter>,
  );
  expect(screen.getByRole("progressbar")).toBeInTheDocument();

  mocks.useKleinJob.mockReturnValue({ job: undefined, error: new Error("missing"), isLoading: false });
  rerender(
    <MemoryRouter>
      <KleinConfigurationPage />
    </MemoryRouter>,
  );
  expect(screen.getByText("Unable to load configuration.")).toBeInTheDocument();
});
