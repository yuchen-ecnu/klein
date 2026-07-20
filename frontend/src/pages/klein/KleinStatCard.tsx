import { Box, Paper, Typography } from "@mui/material";
import React, { ReactNode } from "react";

type KleinStatCardProps = {
  label: string;
  value: ReactNode;
  detail?: ReactNode;
};

export const KleinStatCard = ({ label, value, detail }: KleinStatCardProps) => (
  <Paper
    variant="outlined"
    sx={{ minWidth: 180, flex: "1 1 180px", padding: 2 }}
  >
    <Typography color="text.secondary" variant="body2">
      {label}
    </Typography>
    <Typography sx={{ fontWeight: 500, marginTop: 0.5 }} variant="h5">
      {value}
    </Typography>
    {detail !== undefined && (
      <Box sx={{ color: "text.secondary", fontSize: 12, marginTop: 0.5 }}>
        {detail}
      </Box>
    )}
  </Paper>
);
