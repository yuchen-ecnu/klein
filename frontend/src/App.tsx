import {
  Box,
  IconButton,
  Link,
  MenuItem,
  Select,
  Tooltip,
  Typography,
} from "@mui/material";
import {
  RiBookMarkLine,
  RiDashboardLine,
  RiFeedbackLine,
  RiFileList3Line,
  RiSettings3Line,
} from "react-icons/ri";
import { type ReactNode, useEffect, useState } from "react";
import {
  HashRouter,
  Link as RouterLink,
  Navigate,
  NavLink,
  Outlet,
  Route,
  Routes,
  useParams,
} from "react-router-dom";
import KleinMark from "./assets/KleinMark.svg";
import RayLogo from "./assets/RayLogo.svg";
import { KleinCheckpointsPage } from "./pages/klein/KleinCheckpointsPage";
import { KleinConfigurationPage } from "./pages/klein/KleinConfigurationPage";
import { KleinJobOverviewPage } from "./pages/klein/KleinJobOverviewPage";
import { KleinJobsPage } from "./pages/klein/KleinJobsPage";
import { useKleinJob } from "./pages/klein/hook/useKleinJobs";

const MAIN_NAV_HEIGHT = 56;
const BREADCRUMBS_HEIGHT = 36;
const SIDE_NAV_WIDTH = 56;

const RAY_NAV_ITEMS = [
  ["Overview", "/overview"],
  ["Jobs", "/jobs"],
  ["Serve", "/serve"],
  ["Cluster", "/cluster"],
  ["Actors", "/actors"],
  ["Metrics", "/metrics"],
  ["Logs", "/logs"],
] as const;

export const App = () => (
  <HashRouter>
    <Routes>
      <Route element={<DashboardShell />} path="/">
        <Route element={<Navigate replace to="/klein" />} index />
        <Route element={<KleinJobsPage />} path="klein" />
        <Route element={<KleinJobLayout />} path="klein/jobs/:jobId">
          <Route element={<KleinJobOverviewPage />} index />
          <Route element={<KleinCheckpointsPage />} path="checkpoints" />
          <Route element={<KleinConfigurationPage />} path="configuration" />
        </Route>
        <Route element={<Navigate replace to="/klein" />} path="*" />
      </Route>
    </Routes>
  </HashRouter>
);

const DashboardShell = () => (
  <Box sx={{ background: "#F8FAFC", minHeight: "100vh", width: "100%" }}>
    <MainNav />
    <Box component="main" sx={{ paddingTop: `${MAIN_NAV_HEIGHT}px` }}>
      <Outlet />
    </Box>
  </Box>
);

const MainNav = () => {
  const [rayDashboardUrl, setRayDashboardUrl] = useState(
    "http://127.0.0.1:8265",
  );
  useEffect(() => {
    fetch("api/config")
      .then((response) => response.json())
      .then((config) => {
        if (typeof config.ray_dashboard_url === "string") {
          setRayDashboardUrl(config.ray_dashboard_url.replace(/\/+$/, ""));
        }
      })
      .catch(() => undefined);
  }, []);
  const rayHref = (path: string) => `${rayDashboardUrl}/#${path}`;
  const timeZone = `GMT${new Date().getTimezoneOffset() <= 0 ? "+" : "-"}${Math.abs(
    new Date().getTimezoneOffset() / 60,
  )}`;
  return (
    <Box
      component="nav"
      sx={{
        alignItems: "center",
        backgroundColor: "white",
        boxShadow: "0px 1px 0px #D2DCE6",
        display: "flex",
        flexWrap: "nowrap",
        height: MAIN_NAV_HEIGHT,
        position: "fixed",
        width: "100%",
        zIndex: 1000,
      }}
    >
      <Link
        href={rayHref("/")}
        sx={{ display: "flex", justifyContent: "center", marginLeft: 2, marginRight: 3 }}
      >
        <Box alt="Ray" component="img" src={RayLogo} sx={{ width: 28 }} />
      </Link>
      {RAY_NAV_ITEMS.slice(0, 3).map(([title, path]) => (
        <NavItem href={rayHref(path)} key={path} title={title} />
      ))}
      <Typography>
        <Link
          component={RouterLink}
          sx={{
            alignItems: "center",
            color: "#036DCF",
            display: "flex",
            fontSize: "1rem",
            fontWeight: 750,
            gap: 0.75,
            letterSpacing: "0.035em",
            marginRight: 6,
            textDecoration: "none",
          }}
          to="/klein"
        >
          <Box alt="" aria-hidden component="img" src={KleinMark} sx={{ height: 24, width: 28 }} />
          Klein
        </Link>
      </Typography>
      {RAY_NAV_ITEMS.slice(3).map(([title, path]) => (
        <NavItem href={rayHref(path)} key={path} title={title} />
      ))}
      <Box sx={{ flexGrow: 1 }} />
      <Tooltip title="Docs">
        <IconButton
          href="https://docs.ray.io/en/latest/ray-core/ray-dashboard.html"
          rel="noopener noreferrer"
          sx={{ color: "#5F6469" }}
          target="_blank"
        >
          <RiBookMarkLine />
        </IconButton>
      </Tooltip>
      <Tooltip title="Leave feedback">
        <IconButton
          href="https://github.com/ray-project/ray/issues/new?labels=bug%2Ctriage%2Cdashboard"
          rel="noopener noreferrer"
          sx={{ color: "#5F6469" }}
          target="_blank"
        >
          <RiFeedbackLine />
        </IconButton>
      </Tooltip>
      <Select size="small" sx={{ marginLeft: 1, marginRight: 3, minWidth: 112 }} value={timeZone}>
        <MenuItem value={timeZone}>{timeZone}</MenuItem>
      </Select>
    </Box>
  );
};

const NavItem = ({ href, title }: { href: string; title: string }) => (
  <Typography>
    <Link
      href={href}
      sx={{
        color: "black",
        fontSize: "1rem",
        fontWeight: 500,
        marginRight: 6,
        textDecoration: "none",
      }}
    >
      {title}
    </Link>
  </Typography>
);

const KleinJobLayout = () => {
  const { jobId } = useParams();
  const { job } = useKleinJob(jobId);
  return (
    <>
      <Box
        aria-label="Breadcrumb"
        sx={{
          alignItems: "center",
          backgroundColor: "white",
          boxShadow: "0px 1px 0px #D2DCE6",
          display: "flex",
          gap: 1,
          height: BREADCRUMBS_HEIGHT,
          paddingX: 2,
        }}
      >
        <Link color="#8C9196" component={RouterLink} to="/klein" underline="none">
          Klein
        </Link>
        <Typography color="#8C9196">/</Typography>
        <Typography sx={{ fontWeight: 500 }}>{job?.job_name ?? jobId ?? "Job"}</Typography>
      </Box>
      <Box sx={{ display: "flex", minHeight: `calc(100vh - ${MAIN_NAV_HEIGHT + BREADCRUMBS_HEIGHT}px)` }}>
        <Box
          component="aside"
          sx={{
            background: "white",
            borderRight: "1px solid #D2DCE6",
            flex: `0 0 ${SIDE_NAV_WIDTH}px`,
            paddingTop: 2,
          }}
        >
          <SideNavLink end icon={<RiDashboardLine />} label="Overview" to="" />
          <SideNavLink icon={<RiFileList3Line />} label="Checkpoints" to="checkpoints" />
          <SideNavLink icon={<RiSettings3Line />} label="Configuration" to="configuration" />
        </Box>
        <Box sx={{ minWidth: 0, width: `calc(100% - ${SIDE_NAV_WIDTH}px)` }}>
          <Outlet />
        </Box>
      </Box>
    </>
  );
};

const SideNavLink = ({
  end,
  icon,
  label,
  to,
}: {
  end?: boolean;
  icon: ReactNode;
  label: string;
  to: string;
}) => (
  <Tooltip placement="right" title={label}>
    <Box
      aria-label={label}
      component={NavLink}
      end={end}
      sx={{
        "&.active": { backgroundColor: "#E3F2FD", color: "#036DCF" },
        alignItems: "center",
        color: "#5F6469",
        display: "flex",
        fontSize: 24,
        height: 48,
        justifyContent: "center",
        marginBottom: 1,
        textDecoration: "none",
        width: 48,
      }}
      to={to}
    >
      {icon}
    </Box>
  </Tooltip>
);
