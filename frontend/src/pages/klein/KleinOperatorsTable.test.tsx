import { fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router";
import { expect, it, vi } from "vitest";
import { operatorFixture } from "../../test/fixtures";
import { KleinOperatorsTable } from "./KleinOperatorsTable";

it("supports pointer and keyboard selection without hijacking actor links", async () => {
  const user = userEvent.setup();
  const select = vi.fn();
  render(
    <MemoryRouter>
      <KleinOperatorsTable
        onSelectOperator={select}
        operators={[
          operatorFixture({ op_id: 1, name: "single", parallelism: 1 }),
          operatorFixture({ op_id: 2, name: "parallel", parallelism: 2 }),
        ]}
        selectedOperatorId={2}
      />
    </MemoryRouter>,
  );

  await user.click(screen.getByRole("button", { name: "Select operator single" }));
  fireEvent.keyDown(screen.getByRole("button", { name: "Select operator parallel" }), { key: "Enter" });
  await user.click(screen.getByRole("button", { name: "View 2 actors" }));

  expect(select.mock.calls).toEqual([[1], [2], [2]]);
  expect(screen.getByText("Actor unavailable")).toBeInTheDocument();
});
