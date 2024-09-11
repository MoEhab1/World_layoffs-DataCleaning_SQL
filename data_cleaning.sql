-- Data Cleaning

-- ------------------------------------------------------
-- 1. Importing the data
--    Data source: https://github.com/AlexTheAnalyst/MySQL-YouTube-Series/blob/main/layoffs.csv

SELECT*
FROM layoffs;

-- -----------------------------------------------------------------------------
-- 2. Creating another raw dataset

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT*
FROM layoffs;

SELECT*
FROM layoffs_staging;

-- -----------------------------------------------------------------------------
-- 3. Reomove Duplicates
--    Because there's no unique ID column so we have to create row numbers

SELECT*,
ROW_NUMBER() OVER(PARTITION BY company, location, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT*,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT*
FROM duplicate_cte
WHERE row_num > 1;

/*
To delete duplicate records we can't just do the delete function we can only do it in sql microsoft server like this
WITH duplicate_cte AS
(
SELECT*,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
DELETE
FROM duplicate_cte
WHERE row_num > 1;
*/

-- However we can creat a new table to add the data in it and then we can delete the duplicates

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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT*
FROM layoffs_staging2
WHERE row_num > 1;



INSERT INTO layoffs_staging2
SELECT*,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;


DELETE
FROM layoffs_staging2
WHERE row_num > 1;


SELECT*
FROM layoffs_staging2
WHERE row_num > 1;


SELECT*
FROM layoffs_staging2;

-- -----------------------------------------------------------------------------
-- 4. Standardize the data

-- There's a space befroe the name of the some companies so we have to remove it by using TRIM().
SELECT company, (TRIM(company))
FROM layoffs_staging2;

-- There's a space befroe the name of some companies so we have to remove it by updating the name using TRIM(), UPDATE and SET.
UPDATE layoffs_staging2
SET company = TRIM(company);

-- There's in industry column crypto and, CryptoCurrency which is the same thing so we words must be unified.
SELECT *
FROM layoffs_staging2
WHERE industry LIKE "Crypto%";

UPDATE layoffs_staging2
SET industry = "Crypto"
WHERE industry LIKE "Crypto%";
 
SELECT DISTINCT(industry)
FROM layoffs_staging2
WHERE industry LIKE "Crypto%";

-- There's a dot at the end of United States so we have to update it.
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = "United States"
WHERE country LIKE "United States%";

-- We can also do it by using advanced TRIM() like this
/*
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE "United States%"
*/

-- In order to make time series data analysis and time series data visualization, we have to convert the date into a date format
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

-- Now we need to change the date from text to date

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- -----------------------------------------------------------------------------
-- 5. Null values or blank values

-- it looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What we can do is
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all

SELECT*
FROM layoffs_staging2
WHERE company = "Juul" OR industry IS NULL OR industry = '';

UPDATE layoffs_staging2
SET industry = "Travel"
WHERE company = "Airbnb";

UPDATE layoffs_staging2
SET industry = "Transportation"
WHERE company = "Carvana";

UPDATE layoffs_staging2
SET industry = "Consumer"
WHERE company = "Juul";


/*
-- Or instead of doing it manualy we can do it in a more efficant way. 
-- What we can do write a query that if there is another row with the same company name, 
-- it will update it to the non-null industry values 
-- makes it easy so if there were thousands we wouldn't have to manually check them all

-- we should set the blanks to nulls since those are typically easier to work with
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';


-- now if we check those are all null
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;


-- now we need to populate those nulls if possible
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;


-- and if we check it looks like Bally's was the only one without a populated row to populate this null values
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;
*/


-- -----------------------------------------------------------------------------
-- 6. Remove any columns and rows (not necessary)

SELECT*
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND
	percentage_laid_off IS NULL;
    
-- We have ot delete these rows because there's no info about
-- the total_laid_off and the also the percentage_laid_off 
-- we could simply do some calcaulation and get the the total_laid_off
-- if we have the percentage and the total num of employees but we don'table
-- so now we have to delete the unknown values for the total_laid_off and the percent.. = 361(R)

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND
	percentage_laid_off IS NULL;
    

SELECT*
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT*
FROM layoffs_staging2;
