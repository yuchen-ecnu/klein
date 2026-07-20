import { blueGrey, grey, lightBlue } from "@mui/material/colors";
import { createTheme, ThemeOptions } from "@mui/material/styles";

const basicTheme: ThemeOptions = {
  typography: {
    fontSize: 12,
    fontFamily: [
      "Roboto",
      "-apple-system",
      "BlinkMacSystemFont",
      '"Segoe UI"',
      '"Helvetica Neue"',
      "Arial",
      "sans-serif",
      '"Apple Color Emoji"',
      '"Segoe UI Emoji"',
      '"Segoe UI Symbol"',
    ].join(","),
    h1: { fontSize: "1.5rem", fontWeight: 500 },
    h2: { fontSize: "1.25rem", fontWeight: 500 },
    h3: { fontSize: "1rem", fontWeight: 500 },
    h4: { fontSize: "1rem" },
    body1: { fontSize: "0.75rem" },
    body2: { fontSize: "14px", lineHeight: "20px" },
    caption: { fontSize: "0.75rem", lineHeight: "16px" },
  },
  components: {
    MuiButton: { defaultProps: { size: "small" } },
    MuiTextField: { defaultProps: { size: "small" } },
    MuiPaper: {
      styleOverrides: {
        outlined: { borderColor: "#D2DCE6", borderRadius: 8 },
      },
    },
    MuiTooltip: {
      styleOverrides: {
        tooltip: {
          fontSize: "0.75rem",
          fontWeight: 400,
          boxShadow: "0px 3px 14px 2px rgba(3, 28, 74, 0.12)",
          padding: 8,
        },
      },
    },
  },
};

export const lightTheme = createTheme(basicTheme, {
  palette: {
    primary: { main: "#036DCF" },
    secondary: lightBlue,
    success: { main: "#43A047" },
    error: { main: "#D32F2F" },
    text: {
      primary: grey[900],
      secondary: grey[800],
      disabled: grey[400],
      hint: grey[300],
    },
    background: { paper: "#fff", default: blueGrey[50] },
  },
});
