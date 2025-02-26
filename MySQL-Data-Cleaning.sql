-- Data Cleaning Process

-- Retrieve all records from the initial dataset
SELECT *
FROM layoffs_stagin;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Handle Null or Blank Values
-- 4. Remove Unnecessary Columns

-- Create a staging table with the same structure as the original table
CREATE TABLE layoffs_staging
LIKE layoffs;

-- Insert data into the staging table
INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

-- Identify potential duplicate records using row numbers
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`
       ) AS row_num
FROM layoffs_staging;

-- Using a CTE to flag duplicate records
WITH duplicate_cte AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
           ) AS row_num
    FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Verify specific company data
SELECT * 
FROM layoffs_staging
WHERE company = 'Casper';

-- Remove duplicate records using a CTE approach
WITH duplicate_cte AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
           ) AS row_num
    FROM layoffs_staging
)
DELETE 
FROM layoffs_staging
WHERE row_num > 1;

-- Create a second staging table with a defined schema
CREATE TABLE layoffs_staging2 (
    company TEXT,
    location TEXT,
    industry TEXT,
    total_laid_off INT DEFAULT NULL,
    percentage_laid_off TEXT,
    `date` TEXT,
    stage TEXT,
    country TEXT,
    funds_raised_millions INT DEFAULT NULL,
    row_num INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Check the newly created table
SELECT * 
FROM layoffs_staging2;

-- Insert unique records into the second staging table
INSERT INTO layoffs_staging2
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
       ) AS row_num
FROM layoffs_staging;

-- Identify duplicate records based on the row number
SELECT * 
FROM layoffs_staging2
WHERE row_num > 1;

-- Temporarily disable safe updates to delete duplicate records
SET SQL_SAFE_UPDATES = 0;
DELETE FROM layoffs_staging2 WHERE row_num > 1;
SET SQL_SAFE_UPDATES = 1;

-- Verify the cleaned data
SELECT * 
FROM layoffs_staging2;

-- Standardizing Data

-- Trim spaces from company names
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

-- Identify distinct industry values
SELECT DISTINCT industry
FROM layoffs_staging2
GROUP BY industry;

-- Standardize industry names
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Clean up country names by removing trailing periods
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Convert date column to a proper format
SELECT `date`
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Change the column type to DATE
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE; 

-- Identify records with missing key data
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- Identify specific company records for validation
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

-- Fill missing industry values using related company records
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- Set empty industry fields to NULL
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Update missing industry fields based on other company records
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Final data validation
SELECT *
FROM layoffs_staging2;

-- Identify and remove records with missing key data
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Verify the final dataset
SELECT *
FROM layoffs_staging2;

-- Remove the row_num column as it's no longer needed
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
