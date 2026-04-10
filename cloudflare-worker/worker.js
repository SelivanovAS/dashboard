// Нерабочие праздничные дни РФ на 2026 год (производственный календарь).
// Обновлять ежегодно после публикации постановления Правительства.
const HOLIDAYS_2026 = new Set([
  "01-01", "01-02", "01-03", "01-04", "01-05", "01-06", "01-07", "01-08",
  "02-23",
  "03-09", // перенос с 08.03 (вс) на 09.03 (пн)
  "05-01", "05-04", // перенос с 03.01 на 04.05
  "05-11", // перенос с 09.05 (сб) на 11.05 (пн)
  "06-12",
  "11-04",
]);

function isHoliday(date) {
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const dd = String(date.getDate()).padStart(2, "0");
  const key = `${mm}-${dd}`;
  const year = date.getFullYear();
  const holidays = { 2026: HOLIDAYS_2026 };
  const set = holidays[year];
  return set ? set.has(key) : false;
}

export default {
  async scheduled(event, env) {
    // Текущая дата по МСК (UTC+3)
    const now = new Date(Date.now() + 3 * 3600 * 1000);

    if (isHoliday(now)) {
      console.log(`Пропуск: ${now.toISOString().slice(0, 10)} — праздничный день`);
      return;
    }

    await fetch(
      "https://api.github.com/repos/SelivanovAS/dashboard/actions/workflows/update_cases.yml/dispatches",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${env.GITHUB_PAT}`,
          Accept: "application/vnd.github.v3+json",
          "User-Agent": "CloudflareWorker",
        },
        body: JSON.stringify({ ref: "main" }),
      }
    );
  },
};
