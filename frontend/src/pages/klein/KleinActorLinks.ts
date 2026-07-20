export const getKleinOperatorActorsLink = (
  operatorName: string,
  rayNamespace?: string,
) => {
  const searchParams = new URLSearchParams({
    actorName: `${operatorName} (`,
  });
  if (rayNamespace) {
    searchParams.set("rayNamespace", rayNamespace);
  }
  return `/actors?${searchParams.toString()}`;
};
