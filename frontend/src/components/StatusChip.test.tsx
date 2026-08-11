import { render, screen } from "@testing-library/react";
import { expect, it } from "vitest";
import { StatusChip } from "./StatusChip";

it("renders a known status with optional icon and suffix", () => {
  render(
    <StatusChip
      icon={<span>icon</span>}
      status="RUNNING"
      suffix={<span>2/3</span>}
      type="kleinJob"
    />,
  );

  expect(screen.getByText("RUNNING")).toBeInTheDocument();
  expect(screen.getByText("icon")).toBeInTheDocument();
  expect(screen.getByText("2/3")).toBeInTheDocument();
});

it("renders unknown and non-string statuses safely", () => {
  const { rerender } = render(
    <StatusChip status="PAUSED" type="kleinOperator" />,
  );
  expect(screen.getByText("PAUSED")).toBeInTheDocument();

  rerender(
    <StatusChip status={<strong>custom</strong>} type="kleinCheckpoint" />,
  );
  expect(screen.getByText("custom")).toBeInTheDocument();
});
