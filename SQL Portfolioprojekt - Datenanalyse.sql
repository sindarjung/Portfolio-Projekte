## SQL Projekt - Explorative Datenanalyse
## https://www.kaggle.com/datasets/swaptr/layoffs-2022

## In diesem Abschnitt werden wir die Daten untersuchen, um mögliche Trends, Muster oder auffällige Ausreißer zu identifizieren.
## Zu Beginn einer (explorativen) Datenanalyse hat man in der Regel bereits eine Vorstellung davon, welche Aspekte besonders interessant sein könnten.
## Mit den vorliegenden Informationen wird jedoch zunächst eine offene Erkundung durchführen, um zu sehen, welche Erkenntnisse sich aus den Daten ergeben.

SELECT * 
FROM world_layoffs.layoffs_staging2;

SELECT MAX(total_laid_off)
FROM world_layoffs.layoffs_staging2;

## Untersuchung des prozentualen Anteils, um das Ausmaß der Entlassungen besser zu verstehen.
SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off IS NOT NULL;

## Ermitteln, welche Unternehmen eine Entlassungsquote von 1 aufweisen, was bedeutet, dass 100 % der Belegschaft entlassen wurden.
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1;
## Es scheint sich dabei überwiegend um Start-ups zu handeln, die in diesem Zeitraum ihren Geschäftsbetrieb eingestellt haben.

## Wenn wir nach der Kennzahl funds_raised_millions sortieren, können wir erkennen, wie groß einige dieser Unternehmen waren.
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

## Unternehmen mit den größten einzelnen Entlassungswellen.
SELECT company, total_laid_off
FROM world_layoffs.layoffs_staging
ORDER BY 2 DESC
LIMIT 5;
## Dies bezieht sich lediglich auf einen einzigen Tag.

## Unternehmen mit den höchsten Gesamtentlassungen.
SELECT company, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;

## Nach Standort
SELECT location, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

## Handelt es sich hierbei um die Gesamtsumme der letzten drei Jahre oder um den Gesamtwert im gesamten Datensatz?
SELECT country, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(date), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY YEAR(date)
ORDER BY 1 ASC;

SELECT industry, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT stage, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

## Zuvor haben wurden die Unternehmen mit den meisten Entlassungen angesehen.
## Im Folgenden wird die Entwicklung auf Jahresbasis betrachtet

WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;

## Rollierende Summe der Entlassungen pro Monat.
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC;

## Es wird ein CTE verwendet, damit wir darauf basierend Abfragen durchführen können.
WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;



















































