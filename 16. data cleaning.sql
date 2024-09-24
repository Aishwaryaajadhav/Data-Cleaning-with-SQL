-- Data cleaning

SELECT *
FROM layoffs;

-- 1. remove duplicates
-- 2. standardised data
-- 3. null values or blank values
-- 4. remove any columns 

create table layoffs_staging
like layoffs;

select * 
from layoffs_staging;

insert layoffs_staging
select * 
from layoffs;

select *,
row_number() over(
Partition by company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
from layoffs_staging;

with duplicate_cte as
(
select *,
ROW_NUMBER() over(
Partition by company, location, 
industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) AS row_num
from layoffs_staging
)
Select * 
from duplicate_cte
where row_num > 1;

select * 
from layoffs_staging
where company = 'Casper';




CREATE TABLE `layoffs_staging2` (
  `company` text DEFAULT NULL,
  `location` text DEFAULT NULL,
  `industry` text DEFAULT NULL,
  `total_laid_off` int(11) DEFAULT NULL,
  `percentage_laid_off` text DEFAULT NULL,
  `date` text DEFAULT NULL,
  `stage` text DEFAULT NULL,
  `country` text DEFAULT NULL,
  `funds_raised_millions` int(11) DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

select * 
from layoffs_staging2;

insert into layoffs_staging2
select *, 
ROW_NUMBER() over(
Partition by company, location, 
industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) AS row_num
from layoffs_staging;

select * 
from layoffs_staging2
where row_num > 1;

delete
from layoffs_staging2
where row_num > 1;

select * 
from layoffs_staging2;

-- standardizing data

select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select *
from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct industry
from layoffs_staging2;

select distinct location 
from layoffs_staging2
order by 1;

select distinct country, trim(TRAILING '.' FROM country)
from layoffs_staging2
order by 1;


update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

select * 
from layoffs_staging2;

select `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = STR_TO_DATE(`date`, '%m/%d/%Y');


select date 
from layoffs_staging2;

ALTER Table layoffs_staging2
modify  column `date` DATE;

-- blank values


select *
from layoffs_staging2;

select *
from layoffs_staging2
where industry IS NULL 
OR industry = '';

select *
from layoffs_staging2
where company = 'Airbnb';

select t1.industry, t2.industry
from layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = "")
AND t2.industry IS NOT NULL ;   

update layoffs_staging2
set industry = null 
where industry = "";

update layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
set t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL ;   

select *
from layoffs_staging2
where total_laid_off IS NULL
AND percentage_laid_off IS NULL;

Delete 
from layoffs_staging2
where total_laid_off IS NULL
AND percentage_laid_off IS NULL;

select *
from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;

select *
from layoffs_staging2;














