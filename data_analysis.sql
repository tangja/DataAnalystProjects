-- Exploratory Data Analysis

SELECT *
FROM layoffs_staging2;

-- looking at companies that went under ordered by largest funds earned
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- looking at companies who laid off the most people
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- looking at industries that laid off the most people
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- looking at countries that laid off the most people
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- looking at how many people were laid off by year
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- looking at total lay offs every month
SELECT SUBSTRING(`date`, 1, 7) AS `month`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `month`
ORDER BY 1 ASC;
-- rolling total of lay offs every month
WITH Rolling_Total AS 
(
SELECT SUBSTRING(`date`, 1, 7) AS `month`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `month`
)
SELECT `month`, total_off, SUM(total_off) OVER(ORDER BY `month`) as rolling_total
FROM Rolling_Total
;

-- looking at how many layoffs companies had per year
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;
-- rolling total of lay offs by company per year
WITH CRolling_Total AS
(
SELECT company, YEAR(`date`) AS `year`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
GROUP BY company, `year`
)
SELECT company, `year`, total_off, SUM(total_off) OVER(PARTITION BY company ORDER BY `year`) as crolling_total
FROM CRolling_Total
WHERE total_off IS NOT NULL
;

-- ranking top 5 companies on most laid off by the year
WITH Company_Year AS
(
SELECT company, YEAR(`date`) AS `year`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
GROUP BY company, `year`
), Company_Year_Rank AS
(
SELECT *, DENSE_RANK() OVER(PARTITION BY `year` ORDER BY total_off DESC) AS ranking
FROM Company_Year
WHERE total_off IS NOT NULL
AND `year` IS NOT NULL
-- ORDER BY ranking ASC 
-- use above to order by most laid off per year
)
SELECT *
FROM Company_Year_Rank
WHERE ranking <= 5
;

-- ranking countries for most laid off per year
WITH Country_Year AS
(
SELECT country, YEAR(`date`) AS `year`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
GROUP BY country, `year`
), Country_Year_Rank AS
(
SELECT *, DENSE_RANK() OVER(PARTITION BY `year` ORDER BY total_off DESC) AS ranking
FROM Country_Year
WHERE total_off IS NOT NULL
AND `year` IS NOT NULL
)
SELECT *
FROM Country_Year_Rank
;