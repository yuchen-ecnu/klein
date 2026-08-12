import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ComponentType, ReactNode } from "react";
import { expect, it, vi } from "vitest";
import { operatorFixture } from "../../test/fixtures";
import { KleinJobGraph } from "./KleinJobGraph";

type MockNode = {
  data: unknown;
  id: string;
  selected?: boolean;
  type?: string;
};

vi.mock("@xyflow/react", () => {
  const passthrough = ({ children }: { children?: ReactNode }) => <>{children}</>;
  const useNodesState = <Node,>(initial: Node[]) => [initial, vi.fn(), vi.fn()] as const;
  const useEdgesState = <Edge,>(initial: Edge[]) => [initial, vi.fn(), vi.fn()] as const;
  return {
    Background: () => null,
    BackgroundVariant: { Dots: "dots" },
    BaseEdge: () => null,
    Controls: () => null,
    Handle: () => null,
    MarkerType: { ArrowClosed: "arrow-closed" },
    MiniMap: () => null,
    Panel: passthrough,
    Position: { Left: "left", Right: "right" },
    ReactFlow: ({
      children,
      nodeTypes,
      nodes,
      onNodeClick,
    }: {
      children?: ReactNode;
      nodeTypes: Record<string, ComponentType<Record<string, unknown>>>;
      nodes: MockNode[];
      onNodeClick: (event: unknown, node: MockNode) => void;
    }) => (
      <div>
        {nodes.map((node) => {
          const Node = nodeTypes[node.type ?? ""];
          return (
            <button key={node.id} onClick={() => onNodeClick({}, node)} type="button">
              <Node data={node.data} selected={node.selected} />
            </button>
          );
        })}
        {children}
      </div>
    ),
    useEdgesState,
    useNodesState,
  };
});

it("renders execution roles and opens a selected operator", async () => {
  const user = userEvent.setup();
  const highlight = vi.fn();
  const open = vi.fn();
  render(
    <KleinJobGraph
      edges={[
        { source: 1, target: 2 },
        { source: 2, target: 3 },
      ]}
      highlightedOperatorId={2}
      onHighlightOperator={highlight}
      onOpenOperatorDetails={open}
      operators={[
        operatorFixture({ name: "source", op_id: 1 }),
        operatorFixture({ name: "stateful", op_id: 2, checkpoint_state_size_bytes: 1024 }),
        operatorFixture({ name: "sink", op_id: 3 }),
      ]}
    />,
  );

  expect(screen.getByText(/SOURCE · P2/)).toBeInTheDocument();
  expect(screen.getByText(/STATEFUL · P2/)).toBeInTheDocument();
  expect(screen.getByText(/SINK · P2/)).toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: /Select operator stateful/ }));
  expect(highlight).toHaveBeenCalledWith(2);
  expect(open).toHaveBeenCalledWith(2);
});

it("reports when no execution graph is available", () => {
  render(<KleinJobGraph edges={[]} operators={[]} />);
  expect(screen.getByText("The execution graph is not available yet.")).toBeInTheDocument();
});
