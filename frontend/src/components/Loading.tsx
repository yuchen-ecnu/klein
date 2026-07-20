import { CircularProgress } from "@mui/material";

const Loading = ({ loading }: { loading: boolean }) =>
  loading ? <CircularProgress color="primary" /> : null;

export default Loading;
