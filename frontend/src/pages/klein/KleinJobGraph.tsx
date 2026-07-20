import { graphlib, layout as layoutGraph } from "@dagrejs/dagre";
import {
  Box,
  Chip,
  LinearProgress,
  Paper,
  Stack,
  Typography,
} from "@mui/material";
import * as XYFlow from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import React, { useEffect, useMemo } from "react";
import {
  RiDatabase2Line,
  RiFlowChart,
  RiFunctionLine,
  RiInputMethodLine,
} from "react-icons/ri";
import { StatusChip } from "../../components/StatusChip";
import { KleinOperator } from "../../type/klein";
import { formatByteRate, formatCount, formatRate } from "./KleinFormatUtils";
import { getOperatorNodeColors } from "./KleinJobGraphColors";

type KleinJobGraphProps = {
  operators: KleinOperator[];
  edges: { source: number; target: number }[];
  highlightedOperatorId?: number;
  onHighlightOperator?: (operatorId: number) => void;
  onOpenOperatorDetails?: (operatorId: number) => void;
};

type OperatorRole = "source" | "stateful" | "operator" | "sink";
type OperatorNodeData = {
  operator: KleinOperator;
  role: OperatorRole;
};
type OperatorFlowNode = XYFlow.Node<OperatorNodeData, "kleinOperator">;
type EdgePoint = { x: number; y: number };
type OperatorEdgeData = Record<string, unknown> & { points: EdgePoint[] };
type OperatorFlowEdge = XYFlow.Edge<OperatorEdgeData, "kleinRoute">;
type FlowHandleProps = React.HTMLAttributes<HTMLDivElement> & {
  isConnectable?: boolean;
  position: XYFlow.Position;
  type: "source" | "target";
};

// React Flow 12's root declaration also exports an internal `Handle` type,
// which TypeScript 4.8 incorrectly prefers over the runtime component.
const FlowHandle = (
  XYFlow as unknown as {
    Handle: React.ComponentType<FlowHandleProps>;
  }
).Handle;
const {
  Background,
  BackgroundVariant,
  BaseEdge,
  Controls,
  MarkerType,
  MiniMap,
  Panel,
  Position,
  ReactFlow,
  useEdgesState,
  useNodesState,
} = XYFlow;

// Keep execution nodes deliberately compact. A streaming graph often has many
// ranks, and large monitoring cards make the topology unreadable once fit into
// the viewport. The selected operator's full metrics live directly below the
// graph.
const NODE_WIDTH = 244;
const NODE_HEIGHT = 124;
const RANK_SEPARATION = 72;
// Flink inherits Ant Design's light-theme text hierarchy inside JobGraph
// nodes: strong text at 85% black and metric labels at 45% black.
const NODE_TEXT_COLOR = "rgba(0, 0, 0, 0.85)";
const NODE_SECONDARY_TEXT_COLOR = "rgba(0, 0, 0, 0.45)";

const nodeTypes: XYFlow.NodeTypes = {
  kleinOperator: OperatorNode,
};
const edgeTypes: XYFlow.EdgeTypes = {
  kleinRoute: KleinRouteEdge,
};

export const KleinJobGraph = ({
  operators,
  edges,
  highlightedOperatorId,
  onHighlightOperator,
  onOpenOperatorDetails,
}: KleinJobGraphProps) => {
  const topologyKey = useMemo(
    () =>
      `${operators.map(({ op_id }) => op_id).join(",")}|${edges
        .map(({ source, target }) => `${source}-${target}`)
        .join(",")}`,
    [edges, operators],
  );
  const layout = useMemo(
    () => createFlowElements(operators, edges, highlightedOperatorId),
    [edges, highlightedOperatorId, operators],
  );
  const [nodes, setNodes, onNodesChange] = useNodesState<OperatorFlowNode>(
    layout.nodes,
  );
  const [flowEdges, setFlowEdges, onEdgesChange] =
    useEdgesState<OperatorFlowEdge>(layout.edges);

  useEffect(() => {
    setNodes((currentNodes) =>
      layout.nodes.map((node) => ({
        ...node,
        position:
          currentNodes.find(({ id }) => id === node.id)?.position ??
          node.position,
      })),
    );
    setFlowEdges(layout.edges);
  }, [layout, setFlowEdges, setNodes, topologyKey]);

  if (operators.length === 0) {
    return (
      <Typography color="text.secondary">
        The execution graph is not available yet.
      </Typography>
    );
  }

  return (
    <Box
      aria-label="Klein execution DAG"
      sx={{
        backgroundColor: "#F7F9FB",
        border: "1px solid",
        borderColor: "divider",
        borderRadius: 1,
        height: 660,
        overflow: "hidden",
        ".react-flow__attribution": { display: "none" },
        ".react-flow__controls": {
          border: "1px solid #D5DCE3",
          boxShadow: "0 2px 8px rgba(15, 35, 55, 0.12)",
        },
        ".react-flow__controls-button": {
          borderBottomColor: "#E3E8ED",
          height: 30,
          width: 30,
        },
        ".react-flow__edge-path": {
          filter: "drop-shadow(0 1px 1px rgba(30, 65, 95, 0.12))",
        },
        ".react-flow__minimap": {
          border: "1px solid #D5DCE3",
          borderRadius: 4,
          overflow: "hidden",
        },
      }}
    >
      <ReactFlow<OperatorFlowNode, OperatorFlowEdge>
        colorMode="light"
        defaultEdgeOptions={{
          markerEnd: { color: "#6C849B", type: MarkerType.ArrowClosed },
          style: { stroke: "#8DA0B2", strokeWidth: 1.75 },
          type: "smoothstep",
        }}
        edges={flowEdges}
        edgeTypes={edgeTypes}
        elementsSelectable
        fitView
        fitViewOptions={{ maxZoom: 1, padding: 0.1 }}
        maxZoom={1.8}
        minZoom={0.35}
        nodes={nodes}
        nodesConnectable={false}
        nodesDraggable={false}
        nodesFocusable
        nodeTypes={nodeTypes}
        onEdgesChange={onEdgesChange}
        onNodeClick={(_, node) => {
          const operatorId = Number(node.id);
          onHighlightOperator?.(operatorId);
          onOpenOperatorDetails?.(operatorId);
        }}
        onNodesChange={onNodesChange}
        panOnDrag
        proOptions={{ hideAttribution: true }}
        zoomOnDoubleClick={false}
      >
        <Background
          color="#CFD8E3"
          gap={18}
          size={1}
          variant={BackgroundVariant.Dots}
        />
        <Controls position="bottom-left" showInteractive={false} />
        <MiniMap
          maskColor="rgba(239, 244, 248, 0.72)"
          nodeColor={(node) => nodeColor(node as OperatorFlowNode)}
          nodeStrokeColor={(node) =>
            nodeStrokeColor(node as OperatorFlowNode)
          }
          nodeStrokeWidth={3}
          pannable
          position="bottom-right"
          zoomable
        />
        <Panel position="top-left">
          <Chip
            icon={<RiFlowChart />}
            label="Execution DAG"
            size="small"
            sx={{ backgroundColor: "rgba(255, 255, 255, 0.92)" }}
            variant="outlined"
          />
        </Panel>
      </ReactFlow>
    </Box>
  );
};

// A function declaration is intentional: nodeTypes is initialized at module load.
// eslint-disable-next-line prefer-arrow/prefer-arrow-functions
function OperatorNode({ data, selected }: XYFlow.NodeProps<OperatorFlowNode>) {
  const { operator, role } = data;
  const nodeColors = getOperatorNodeColors(operator);
  const busy = nodeColors.busyPercent;
  const backpressure = nodeColors.backpressurePercent;
  const queuePercent =
    operator.capacity > 0
      ? Math.min(100, (operator.queued / operator.capacity) * 100)
      : 0;
  const pressureColor =
    backpressure >= 50 || queuePercent >= 90
      ? "#D9363E"
      : backpressure > 0 || queuePercent >= 60
      ? "#D97706"
      : "#2F80C9";

  return (
    <Paper
      aria-label={`Select operator ${operator.name}, ${busy.toFixed(
        1,
      )}% busy, ${backpressure.toFixed(1)}% backpressured`}
      data-backpressure-percent={backpressure}
      data-busy-percent={busy}
      data-testid={`klein-operator-${operator.op_id}`}
      elevation={0}
      sx={{
        backgroundColor: nodeColors.background,
        border: `1px solid ${nodeColors.border}`,
        borderRadius: 0,
        boxShadow: "none",
        color: NODE_TEXT_COLOR,
        height: NODE_HEIGHT,
        overflow: "hidden",
        transition:
          "background-color 160ms ease, border-color 120ms ease, transform 120ms ease",
        transform: selected ? "scale(1.2)" : "scale(1)",
        width: NODE_WIDTH,
      }}
    >
      <FlowHandle
        isConnectable={false}
        position={Position.Left}
        style={{
          background: nodeColors.border,
          border: "2px solid #FFFFFF",
          height: 9,
          left: -5,
          width: 9,
        }}
        type="target"
      />
      <Box
        sx={{
          alignItems: "center",
          backgroundColor: "transparent",
          display: "flex",
          height: 38,
          paddingX: 1.25,
        }}
      >
        <Box
          sx={{
            alignItems: "center",
            color: NODE_TEXT_COLOR,
            display: "flex",
            fontSize: 17,
            marginRight: 1,
          }}
        >
          <RoleIcon role={role} />
        </Box>
        <Typography
          noWrap
          sx={{ flex: 1, fontWeight: 600 }}
          title={operator.name}
          variant="body2"
        >
          {operator.name}
        </Typography>
        <StatusChip
          style={{
            backgroundColor: "transparent",
            borderColor: "rgba(0, 0, 0, 0.2)",
            color: NODE_SECONDARY_TEXT_COLOR,
          }}
          type="kleinOperator"
          status={operator.status}
        />
      </Box>
      <Box sx={{ padding: 1.1, paddingBottom: 0.65 }}>
        <Stack direction="row" justifyContent="space-between">
          <Typography color={NODE_SECONDARY_TEXT_COLOR} variant="caption">
            {role.toUpperCase()} · P{operator.parallelism}
          </Typography>
          <Typography color={NODE_SECONDARY_TEXT_COLOR} variant="caption">
            ID {operator.op_id}
          </Typography>
        </Stack>
        <Stack direction="row" spacing={1.1} sx={{ marginTop: 0.55 }}>
          <Metric label="IN" value={formatCount(operator.rows_in)} />
          <Metric label="OUT" value={formatCount(operator.rows_out)} />
          <Metric label="BUSY" value={`${busy.toFixed(1)}%`} />
          <Metric label="BACKPRESSURE" value={`${backpressure.toFixed(1)}%`} />
        </Stack>
        <Stack
          alignItems="center"
          direction="row"
          justifyContent="space-between"
          sx={{ marginTop: 0.45 }}
        >
          <Typography
            color={NODE_SECONDARY_TEXT_COLOR}
            title="Estimated logical payload output rate"
            variant="caption"
          >
            {formatByteRate(operator.bytes_out_per_second)} out
          </Typography>
          <Typography
            sx={{
              color: NODE_SECONDARY_TEXT_COLOR,
              fontWeight: queuePercent >= 90 ? 700 : 400,
            }}
            variant="caption"
          >
            Queue {operator.queued}/{operator.capacity || "∞"}
          </Typography>
        </Stack>
        <LinearProgress
          sx={{
            backgroundColor: "#E6ECF2",
            borderRadius: 2,
            height: 4,
            marginTop: 0.35,
            "& .MuiLinearProgress-bar": { backgroundColor: pressureColor },
          }}
          value={Math.max(backpressure, queuePercent)}
          variant="determinate"
        />
      </Box>
      <FlowHandle
        isConnectable={false}
        position={Position.Right}
        style={{
          background: nodeColors.border,
          border: "2px solid #FFFFFF",
          height: 9,
          right: -5,
          width: 9,
        }}
        type="source"
      />
    </Paper>
  );
}

// Dagre already calculates a routed polyline for every edge while minimizing
// crossings. Rendering those control points is the important part of Flink's
// graph implementation; asking React Flow to route the edge again discards the
// layout result and creates avoidable overlaps around fan-in/fan-out nodes.
// eslint-disable-next-line prefer-arrow/prefer-arrow-functions
function KleinRouteEdge({
  data,
  id,
  label,
  labelBgPadding,
  labelBgStyle,
  labelStyle,
  markerEnd,
  style,
}: XYFlow.EdgeProps<OperatorFlowEdge>) {
  const points = data?.points ?? [];
  if (points.length < 2) {
    return null;
  }
  const path = points
    .map(({ x, y }, index) => `${index === 0 ? "M" : "L"} ${x} ${y}`)
    .join(" ");
  const labelPoint = getPolylineMidpoint(points);
  return (
    <BaseEdge
      id={id}
      label={label}
      labelBgPadding={labelBgPadding}
      labelBgStyle={labelBgStyle}
      labelStyle={labelStyle}
      labelX={labelPoint.x}
      labelY={labelPoint.y}
      markerEnd={markerEnd}
      path={path}
      style={style}
    />
  );
}

const RoleIcon = ({ role }: { role: OperatorRole }) => {
  if (role === "source") {
    return <RiInputMethodLine />;
  }
  if (role === "sink") {
    return <RiDatabase2Line />;
  }
  if (role === "stateful") {
    return <RiDatabase2Line />;
  }
  return <RiFunctionLine />;
};

const Metric = ({ label, value }: { label: string; value: string }) => (
  <Box sx={{ minWidth: 0 }}>
    <Typography
      color={NODE_SECONDARY_TEXT_COLOR}
      display="block"
      sx={{ fontSize: 9, letterSpacing: 0.25 }}
    >
      {label}
    </Typography>
    <Typography
      color={NODE_SECONDARY_TEXT_COLOR}
      noWrap
      sx={{ fontWeight: 600 }}
      variant="body2"
    >
      {value}
    </Typography>
  </Box>
);

const createFlowElements = (
  operators: KleinOperator[],
  edges: { source: number; target: number }[],
  highlightedOperatorId?: number,
) => {
  const graph = new graphlib.Graph();
  graph.setGraph({
    align: "DL",
    acyclicer: "greedy",
    edgesep: 72,
    marginx: 20,
    marginy: 20,
    nodesep: 46,
    rankdir: "LR",
    ranker: "network-simplex",
    ranksep: RANK_SEPARATION,
  });
  graph.setDefaultEdgeLabel(() => ({}));

  const incoming = new Map<number, number>();
  const outgoing = new Map<number, number>();
  operators.forEach(({ op_id }) => {
    incoming.set(op_id, 0);
    outgoing.set(op_id, 0);
    graph.setNode(String(op_id), { height: NODE_HEIGHT, width: NODE_WIDTH });
  });
  edges.forEach(({ source, target }) => {
    incoming.set(target, (incoming.get(target) ?? 0) + 1);
    outgoing.set(source, (outgoing.get(source) ?? 0) + 1);
  });
  edges.forEach(({ source, target }) => {
    const weight =
      (outgoing.get(source) ?? 0) > 1
        ? 8
        : (incoming.get(target) ?? 0) > 1
        ? 1
        : 3;
    graph.setEdge(String(source), String(target), { weight });
  });
  layoutGraph(graph);

  const lineage = getOperatorLineage(operators, edges, highlightedOperatorId);
  const flowNodes: OperatorFlowNode[] = operators.map((operator) => {
    const position = graph.node(String(operator.op_id));
    const role: OperatorRole =
      (incoming.get(operator.op_id) ?? 0) === 0
        ? "source"
        : (outgoing.get(operator.op_id) ?? 0) === 0
        ? "sink"
        : operator.checkpoint_state_size_bytes > 0
        ? "stateful"
        : "operator";
    return {
      ariaLabel: `${role} operator ${operator.name}`,
      data: { operator, role },
      id: String(operator.op_id),
      position: {
        x: position.x - NODE_WIDTH / 2,
        y: position.y - NODE_HEIGHT / 2,
      },
      selected: operator.op_id === highlightedOperatorId,
      style: {
        opacity: lineage.has(operator.op_id) ? 1 : 0.42,
        transition: "opacity 140ms ease",
      },
      type: "kleinOperator",
    };
  });

  const operatorById = new Map(
    operators.map((operator) => [operator.op_id, operator]),
  );
  const flowEdges: OperatorFlowEdge[] = edges.map(({ source, target }) => {
    const sourceOperator = operatorById.get(source);
    const dagreEdge = graph.edge(String(source), String(target));
    const recordRate = sourceOperator?.rows_out_per_second ?? 0;
    const byteRate = sourceOperator?.bytes_out_per_second ?? 0;
    const hasFlow = recordRate > 0 || byteRate > 0;
    const isSelectedEdge =
      highlightedOperatorId === undefined ||
      (lineage.has(source) && lineage.has(target));
    const isAdjacentToSelection =
      highlightedOperatorId === source || highlightedOperatorId === target;
    const stroke = isSelectedEdge ? "#2F80C9" : "#C4CED8";
    return {
      animated: isAdjacentToSelection && hasFlow,
      id: `${source}-${target}`,
      label:
        isAdjacentToSelection && hasFlow
          ? `${formatRate(recordRate)} records/s · ${formatByteRate(byteRate)}`
          : undefined,
      labelBgPadding: [5, 3],
      labelBgStyle: { fill: "#F7F9FB", fillOpacity: 0.94 },
      labelStyle: { fill: "#315B7D", fontSize: 10, fontWeight: 600 },
      markerEnd: { color: stroke, type: MarkerType.ArrowClosed },
      data: { points: dagreEdge.points as EdgePoint[] },
      source: String(source),
      style: {
        opacity: isSelectedEdge ? 1 : 0.42,
        stroke,
        strokeWidth: isSelectedEdge ? 2.4 : 1.35,
      },
      target: String(target),
      type: "kleinRoute",
    };
  });
  return { edges: flowEdges, nodes: flowNodes };
};

const nodeColor = (node: OperatorFlowNode) => {
  return getOperatorNodeColors(node.data.operator).background;
};

const nodeStrokeColor = (node: OperatorFlowNode) => {
  return getOperatorNodeColors(node.data.operator).border;
};

const getPolylineMidpoint = (points: EdgePoint[]) => {
  const segments = points.slice(1).map((point, index) => {
    const previous = points[index];
    return {
      end: point,
      length: Math.hypot(point.x - previous.x, point.y - previous.y),
      start: previous,
    };
  });
  const halfLength =
    segments.reduce((total, { length }) => total + length, 0) / 2;
  let traversed = 0;
  for (const segment of segments) {
    if (traversed + segment.length >= halfLength) {
      const ratio =
        segment.length === 0 ? 0 : (halfLength - traversed) / segment.length;
      return {
        x: segment.start.x + (segment.end.x - segment.start.x) * ratio,
        y: segment.start.y + (segment.end.y - segment.start.y) * ratio,
      };
    }
    traversed += segment.length;
  }
  return points[0];
};

const getOperatorLineage = (
  operators: KleinOperator[],
  edges: { source: number; target: number }[],
  highlightedOperatorId?: number,
) => {
  if (highlightedOperatorId === undefined) {
    return new Set(operators.map(({ op_id }) => op_id));
  }
  const upstream = new Map<number, number[]>();
  const downstream = new Map<number, number[]>();
  edges.forEach(({ source, target }) => {
    upstream.set(target, [...(upstream.get(target) ?? []), source]);
    downstream.set(source, [...(downstream.get(source) ?? []), target]);
  });
  const lineage = new Set<number>([highlightedOperatorId]);
  const visit = (start: number, adjacency: Map<number, number[]>) => {
    const pending = [start];
    while (pending.length > 0) {
      const current = pending.pop();
      if (current === undefined) {
        continue;
      }
      (adjacency.get(current) ?? []).forEach((operatorId) => {
        if (!lineage.has(operatorId)) {
          lineage.add(operatorId);
          pending.push(operatorId);
        }
      });
    }
  };
  visit(highlightedOperatorId, upstream);
  visit(highlightedOperatorId, downstream);
  return lineage;
};
