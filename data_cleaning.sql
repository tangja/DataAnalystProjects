-- Data cleaning
-- Database is layoffs around the world from 2020-03 to 2023-03

USE world_layoffs;
SELECT *
FROM layoffs;

-- TODO
-- 1. Remove Duplicates
-- 2. Standardize Data 
-- 3. Null/Blank values (check if they can be populated)
-- 4. Remove any Columns

-- copy data into a staging table to prevent working on raw data
CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging;

-- 1. Removing Duplicates *********
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- create a new staging table to add row_num -> indicator for any data that is not DISTINCT
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- copy data from staging1 and fill row_num
INSERT INTO layoffs_staging2
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM layoffs_staging
;

-- check for dupes
SELECT *
FROM layoffs_staging2
WHERE row_num >1;

-- delete all dupes from staging2
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- confirm dupes are gone
SELECT *
FROM layoffs_staging2
WHERE row_num >1;

-- 2. Stadardizing Data *********

-- take off white space
UPDATE layoffs_staging2
SET company = TRIM(company);

-- check for same industries that have different labels
SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER BY 1;

-- set all Crypto related industries to Crypto
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- check for problems in location
SELECT DISTINCT(location)
FROM layoffs_staging2
ORDER BY 1;

-- check for problems in country -> found same country but with a period
SELECT DISTINCT(country)
FROM layoffs_staging2
ORDER BY 1;

-- remove the period at the end of any countries
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE '%.';

-- change date into correct format to change into date data type
-- second param of STR_TO_DATE is the format that original date is in
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');
-- never use ALTER TABLE on original data
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. NULLS/Blanks *********

-- look for missing data that may be populatable
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';
-- populate nulls/blank based on data
-- replace all blanks in industry with null
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- check if companies with no industries have other inputs where industry is populated
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Airbnb';

SELECT * 
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- update all company industries that are null but have other inputs where the industry is populated
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;
-- will not populate other columns bc there is not enough information in the data to do so

-- 4. Remove Columns *******

-- data that meets these conditions will likely not be helpful 
-- (in professional setting must be 100% before deleting data)
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- drop row_num column
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;
-- cleaned data is in layoffs_staging2