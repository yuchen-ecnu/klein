import dayjs from "dayjs";
import timezone from "dayjs/plugin/timezone";
import utc from "dayjs/plugin/utc";

dayjs.extend(utc);
dayjs.extend(timezone);

export const formatDuration = (durationInSeconds: number) => {
  const normalizedSeconds =
    Number.isFinite(durationInSeconds) && durationInSeconds > 0
      ? Math.floor(durationInSeconds)
      : 0;
  const durationSeconds = normalizedSeconds % 60;
  const durationMinutes = Math.floor(normalizedSeconds / 60) % 60;
  const durationHours = Math.floor(normalizedSeconds / 60 / 60) % 24;
  const durationDays = Math.floor(normalizedSeconds / 60 / 60 / 24);
  const pad = (value: number) => value.toString().padStart(2, "0");
  return [
    durationDays ? `${durationDays}d` : "",
    `${pad(durationHours)}h`,
    `${pad(durationMinutes)}m`,
    `${pad(durationSeconds)}s`,
  ]
    .filter(Boolean)
    .join(" ");
};

export const formatDateFromTimeMs = (time: number) =>
  dayjs.utc(time).tz().format("YYYY/MM/DD HH:mm:ss");
