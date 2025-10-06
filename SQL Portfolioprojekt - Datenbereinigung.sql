## SQL Project - Datenbereinigung
## https://www.kaggle.com/datasets/swaptr/layoffs-2022

SELECT * 
FROM world_layoffs.layoffs;

## Zunächst soll eine Staging-Tabelle erstellt werden. 
## Diese dient als Arbeitsbereich zur Bereinigung der Daten. 
## Es wird eine Tabelle mit den Rohdaten angelegt, um im Falle eines Problems auf die Originaldaten zurückgreifen zu können.
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;


## Bei der Datenbereinigung wird üblicherweise ein mehrstufiger Prozess verfolgt:
# 1. Prüfung auf Duplikate und deren Entfernung.
# 2. Standardisierung der Daten und Korrektur von Fehlern.
# 3. Analyse fehlender Werte und Bewertung ihres Einflusses.
# 4. Entfernung nicht benötigter Spalten und Zeilen, wobei verschiedene Methoden zur Anwendung kommen können.



## 1. Duplikate entfernen
# Zunächst erfolgt eine Prüfung auf Duplikate.

SELECT *
FROM world_layoffs.layoffs_staging
;

SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`) AS row_num
	FROM 
		world_layoffs.layoffs_staging;

SELECT *
FROM (
	SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;
    
## Zur Bestätigung wird ein Blick auf 'Oda' geworfen.
SELECT *
FROM world_layoffs.layoffs_staging
WHERE company = 'Oda'
;
# Es scheint, dass es sich hierbei um gültige Einträge handelt, die nicht gelöscht werden sollten. 
# Eine sorgfältige Prüfung jeder einzelnen Zeile ist erforderlich, um Genauigkeit zu gewährleisten.

## Dies sind die tatsächlichen Duplikate.
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;

## Zu löschen sind die Einträge, deren Zeilennummer größer als 1 oder 2 ist.

## Dies könnte alternativ wie folgt formuliert werden:
WITH DELETE_CTE AS 
(
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1
)
DELETE
FROM DELETE_CTE
;


WITH DELETE_CTE AS (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, 
    ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM world_layoffs.layoffs_staging
)
DELETE FROM world_layoffs.layoffs_staging
WHERE (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num) IN (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num
	FROM DELETE_CTE
) AND row_num > 1;

## Eine mögliche Lösung besteht darin, eine neue Spalte zu erstellen und dort die Zeilennummern einzutragen.  
## Anschließend werden alle Zeilen gelöscht, deren Zeilennummer größer als 2 ist, und die Spalte wieder entfernt.

ALTER TABLE world_layoffs.layoffs_staging ADD row_num INT;


SELECT *
FROM world_layoffs.layoffs_staging
;

CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

INSERT INTO `world_layoffs`.`layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging;

## Nachdem diese Spalte erstellt wurde, können alle Zeilen gelöscht werden, deren row_num größer als 2 ist.

DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num >= 2;



## 2. Daten standardisieren

SELECT * 
FROM world_layoffs.layoffs_staging2;

## Bei Betrachtung der Branche (industry) sind einige Null- oder Leerwerte vorhanden. Diese sollen überprüft werden.
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

## Diese Einträge werden nun näher betrachtet.
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'Bally%';
## Es liegen keine Auffälligkeiten vor.
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'airbnb%';

## Es scheint, dass bei Airbnb die Branche (industry) als "Travel" angegeben ist, während bei diesem Eintrag kein Wert vorhanden ist.
## Vermutlich verhält es sich bei weiteren Einträgen ähnlich.  
## Eine mögliche Vorgehensweise besteht darin, eine Abfrage zu schreiben,  die bei Vorhandensein eines weiteren Eintrags mit demselben Firmennamen  
## den fehlenden Branchenwert durch einen vorhandenen, nicht-leeren Wert ersetzt.  
## Dies vereinfacht den Vorgang erheblich und vermeidet manuelle Überprüfungen bei einer großen Datenmenge.

## Leere Werte sollten in Nullwerte umgewandelt werden, da mit diesen in der Regel einfacher gearbeitet werden kann.
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

## Eine anschließende Überprüfung zeigt, dass alle Werte nun Null sind.

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

## Anschließend müssen die Nullwerte, sofern möglich, mit entsprechenden Werten gefüllt werden.

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

## Eine Überprüfung zeigt, dass Bally's das einzige Unternehmen ohne einen befüllten Eintrag ist, um die Nullwerte zu ergänzen.
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- ---------------------------------------------------

# Es wurde festgestellt, dass der Begriff „Crypto“ in mehreren Varianten vorliegt.  
# Diese sollten standardisiert werden, beispielsweise durch Vereinheitlichung aller Einträge auf „Crypto“.
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

## Dieser Schritt wurde nun abgeschlossen:
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

-- --------------------------------------------------

## Zusätzlich ist eine Überprüfung erforderlich von...

SELECT *
FROM world_layoffs.layoffs_staging2;

# Abgesehen von einigen Fällen, bei denen „United States“ mit einem abschließenden Punkt („United States.“) vorliegt, sind die Daten in Ordnung.  
# Eine Standardisierung ist erforderlich.
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

## Nach erneuter Ausführung ist das Problem behoben.
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;


## Ebenso sind die Datums-Spalten zu korrigieren:
SELECT *
FROM world_layoffs.layoffs_staging2;

# Die Umwandlung dieser Spalte kann mithilfe von str-to-date erfolgen.
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

# Anschließend kann der Datentyp korrekt konvertiert werden.
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT *
FROM world_layoffs.layoffs_staging2;



## 3. Analyse der Nullwerte

## Die Nullwerte in den Spalten total_laid_off, percentage_laid_off und funds_raised_millions erscheinen unproblematisch.  
## Eine Änderung ist nicht erforderlich, da Nullwerte die Berechnungen während der Datenanalyse-Phase erleichtern.

## Es bestehen keine Änderungen an den Nullwerten.



## 4. Entfernen nicht benötigter Spalten und Zeilen.

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;


SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

## Unnütze Daten löschen, die nicht sinnvoll verwendet werden können.
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM world_layoffs.layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


SELECT * 
FROM world_layoffs.layoffs_staging2;


































