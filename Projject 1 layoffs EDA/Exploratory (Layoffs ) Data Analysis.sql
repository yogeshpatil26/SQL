
-- We had created Database "world_layoffs" and Imported data from Flat file (.csv) as table "layoffs" 
use world_layoffs;

select * from layoffs;

-- Initially we are doing CRUD operations we created Backup table (stagging) so ORGINAL table will not be impacted

		select * into layoff_staging from layoffs;

		select * from layoff_staging


-- 1. Remove Duplicate
-- 2. Standardize the data
-- 3. Null values or Blank Values
-- 4. Remove Any columns


-- 1. Remove duplicates
	
	-- First get Duplicate records
	with duplicate_cte as
	(
	select * , ROW_NUMBER() over 
	(Partition by company, location, industry, total_laid_off, percentage_laid_off, [date], stage, country, funds_raised_millions  ORDER BY (SELECT NULL)) as row_num
	from layoff_staging
	)
	select * from duplicate_cte where row_num>1;
	
	-- Now create a new table and add complete data from above into new table
			CREATE TABLE [dbo].[layoff_staging2](
				[company] [varchar](50) NULL,
				[location] [varchar](50) NULL,
				[industry] [varchar](50) NULL,
				[total_laid_off] [varchar](50) NULL,
				[percentage_laid_off] [varchar](50) NULL,
				[date] [varchar](50) NULL,
				[stage] [varchar](50) NULL,
				[country] [varchar](50) NULL,
				[funds_raised_millions] [varchar](50) NULL,
				[row_num] [int] NULL,
			) ON [PRIMARY]

			select * from layoff_staging2
			 
			 -- Insert into new table
				Insert into layoff_staging2 
				select * , ROW_NUMBER() over 
				(Partition by company, location, industry, total_laid_off, percentage_laid_off, [date], stage, country, funds_raised_millions  
				ORDER BY (SELECT NULL)) as row_num
				from layoff_staging;

				select * from layoff_staging2

			-- Now delete the duplicate values
				
				select * from layoff_staging2 where row_num>1

				select * from layoff_staging2 where company = 'Casper'

			-- Delete duplicates row by row number column
				delete from layoff_staging2 where row_num>1

				select * from layoff_staging2 where row_num>1

-- 2. Standardize the data (Column by column standardization)

	-- company column	
			-- company column have some spaces at left side
			select Company, LTRIM(company) from layoff_staging2;

			-- Updated Company with removing left spaces
			update layoff_staging2 
			set company = LTRIM(company);

	-- Industry column
		select distinct industry from layoff_staging2 order by 1;

			-- Data have some industry like 'Crypto', 'CryptoCurrency' and 'Crypto Currency'
			select distinct industry from layoff_staging2 where industry like 'Crypto%';

			-- Updated industry as 'Crypto'
			update layoff_staging2 
			set industry = 'Crypto'
			where industry like 'Crypto%'

	-- Location
			select distinct location from layoff_staging2 order by 1;

	-- Country
			select distinct country from layoff_staging2 order by 1;

		-- 2 records are 'United States' and 'United States.'
			select distinct country from layoff_staging2 where country like 'United%';

			SELECT distinct Country, REPLACE(Country, '.', '') AS CleanedCountry FROM layoff_staging2 
			where country like 'United States%';

			-- Updated and removed . at end 
			update layoff_staging2
			set country = Replace(Country, '.', '') 
			where country like 'United States%';

	-- Date columns
			-- Date datatype is CHAR
			select date from layoff_staging2;
			
			-- Converting CHAR to 'Date FORMAT'
			SELECT Date, TRY_CONVERT(DATE, Date, 101) AS ConvertedDate FROM layoff_staging2;

			-- Updating date column (It is still in VARCHAR but formated as YYYY-MM-DD)
			Update layoff_staging2 set date = TRY_CONVERT(DATE, Date, 101)

			-- Modifying Date column datatype (Now it datatype is DATE)
			ALTER TABLE layoff_staging2
			ALTER COLUMN date DATE;

			SELECT * FROM layoff_staging2;
			

-- 3. Null values or Blank Values
	
			-- Industry column have nulls and Blanks
							SELECT * FROM layoff_staging2 WHERE industry like 'NULL'OR industry = '';

							-- Observed that Airbnb have industry travel and Blank as well
							select * from layoff_staging2 where company = 'Airbnb'

							-- Doing self join to check where we can update the industry column on basis of company
							select * from layoff_staging2 l1 join layoff_staging2 l2 on l1.company=l2.company 
							where l1.company = 'Airbnb';

							select l1.company, l1.industry, l2.company, l2.industry from layoff_staging2 l1 join layoff_staging2 l2 on l1.company=l2.company 
							where l1.company = 'Airbnb';


							begin transaction; 

							update layoff_staging2 
							set industry = 'NULL'
							where industry = '';

							rollback;

							-- Updated Industry column (Null or Blank) with values industry (not null) on basis of company
							UPDATE l1
							SET l1.industry = l2.industry
							FROM layoff_staging2 l1
							JOIN layoff_staging2 l2
							ON l1.company = l2.company
							WHERE l1.industry like 'NULL'
							  AND l2.industry not like 'NULL'

							select * from layoff_staging2 l1 join layoff_staging2 l2 on l1.company=l2.company 
							where l1.company = 'Airbnb';

							select * from layoff_staging2 where company = 'Airbnb'

				
							select * from layoff_staging2 where industry like 'NULL' or Industry = ''

							-- there is only one row (company = 'Bally's Interactive') that have no another reference row for update
							select * from layoff_staging2 where company like 'Bally%'

			
			-- Null values in Tota_laid_off and Percentage_laid_off columns
				
				select * from layoff_staging2 where total_laid_off Like 'NULL' and percentage_laid_off like 'NULL';

				select * from layoff_staging2 where total_laid_off = '' and percentage_laid_off = '';


			-- deleting the null values data
				begin transaction

				delete from layoff_staging2 where total_laid_off Like 'NULL' and percentage_laid_off like 'NULL';
				
				select * from layoff_staging2 where total_laid_off Like 'NULL' and percentage_laid_off like 'NULL';

				rollback;
				commit; 


-- 4. Remove Any columns
				alter table layoff_staging2
				drop column row_num;


-- Exploratory Data Analysis

			-- SUM is not working because the total_laid_off and some others columns have VARCHAR format 
			select industry, sum(cast(total_laid_off as int)) as laid_off from layoff_staging2 
			group by industry
			order by laid_off desc

			-- total laid off , percentage laid off and other columns are in VARCHAR format so converting them into Numeric
			begin transaction;

			update layoff_staging2 
			set total_laid_off = cast(total_laid_off as int) -- Error because NULL values 

			-- since error appears we first have to convert NULLS and Blanks as 0
			UPDATE layoff_staging2
			SET total_laid_off = 
				 CASE 
					 WHEN total_laid_off IS NULL OR LTRIM(RTRIM(total_laid_off)) = '' OR total_laid_off = 'NULL' 
					 THEN 0  -- Replace with default value
					 ELSE CAST(total_laid_off AS INT)
				 END;

			select * from layoff_staging2 where total_laid_off like 'NULL'

			select * from layoff_staging2 where total_laid_off is NULL
			
			-- Converting datatype as INT
			ALTER TABLE layoff_staging2
			ALTER COLUMN total_laid_off int;

			rollback;

			-- Similarly changing it for other columns like percentage_laid_columns, Funds raised millions, 
			begin transaction;

		   UPDATE layoff_staging2
			SET percentage_laid_off = 
			 CASE 
				WHEN percentage_laid_off IS NULL 
				 OR LTRIM(RTRIM(percentage_laid_off)) = '' 
				OR percentage_laid_off = 'NULL'
				THEN '0'  -- Keep it as VARCHAR, but represent as '0' string
				ELSE CAST(CAST(percentage_laid_off AS DECIMAL(5, 2)) AS VARCHAR(10))  -- Cast to decimal with precision, then back to VARCHAR
			END;

			
			select * from layoff_staging2 order by company

			select * from layoff_staging2 where percentage_laid_off = 'NULL' order by company

			rollback;


				begin transaction;

			alter table layoff_staging2
			alter column percentage_laid_off decimal(5,2);

				rollback;

			-- get top 5 indusry  and country by Total layoffs
			select top 5 industry, sum(cast(total_laid_off as int)) as laid_off from layoff_staging2 
			group by industry
			order by laid_off desc
			
			select top 5 country, sum(cast(total_laid_off as int)) as laid_off from layoff_staging2 
			group by country
			order by laid_off desc

			-- Get bottom 5 indsutry and country by Total layoffs
			select industry, sum(total_laid_off) as total_laid_off from layoff_staging2
			group by industry
			order by total_laid_off asc
			offset 0 rows fetch next 5 rows only

			select country, sum(total_laid_off) as total_laid_off from layoff_staging2
			group by country
			order by total_laid_off asc
			offset 0 rows fetch next 5 rows only
			
			-- Get top 3rd to 6  indsutry and country by Total layoffs
			select country, sum(total_laid_off) as total_laid_off from layoff_staging2
			group by country
			order by total_laid_off desc
			offset 2 rows fetch next 5 rows only;


			select country, sum(total_laid_off) as total_laid_off from layoff_staging2
			group by country
			order by total_laid_off desc
			offset 0 rows fetch next 10 rows only;

			-- Layoffs by years
			select year(date), sum(total_laid_off) as total_laid_off from layoff_staging2
			group by year(date) 
			order by 1 desc;

			-- Layoffs by stage of company
			select stage, sum(total_laid_off) as total_laid_off from layoff_staging2
			group by stage
			order by 1 desc;

			-- Extract total laid off by months 
			select month(date) as month_num, sum(total_laid_off) as total_laid_off from layoff_staging2
			group by month(date) 
			order by total_laid_off desc

			-- Extract total laid off with 'YEAR-MM'
			select Format(date, 'yyyy-MM') as Years_months, sum(total_laid_off) as total_laid_off from layoff_staging2
			group by Format(date, 'yyyy-MM')
			order by Years_months desc


			select * from layoff_staging2


			-- Getting cumulative MONTH-TO-DATE values
			with cumulative_total as (
			select Format(date, 'yyyy-MM') as Years_months, sum(total_laid_off) as total_laid_off from layoff_staging2
			group by Format(date, 'yyyy-MM')
			)

			select Years_months, total_laid_off, sum(total_laid_off) over (order by years_months) as rolling_total from cumulative_total
			order by Years_months desc

			-- We got error with above query
				--Msg 8729, Level 16, State 1, Line 1
				--ORDER BY list of RANGE window frame has total size of 8000 bytes. Largest size supported is 900 bytes


			-- So the solution (Found from chatgpt) is to create new column that could be filtered to order (min (date))

				-- Getting cumulative MONTH-TO-DATE values 
					WITH cumulative_total AS (
						SELECT FORMAT(date, 'yyyy-MM') AS Years_months, 
						SUM(total_laid_off) AS total_laid_off,
						MIN(date) AS min_date -- Using the minimum date of each group for ordering
						FROM layoff_staging2
						GROUP BY FORMAT(date, 'yyyy-MM')
					)

					SELECT Years_months, total_laid_off, 
					SUM(total_laid_off) OVER (ORDER BY min_date) AS rolling_total
					FROM cumulative_total where Years_months not like 'NULL'

				-- Find the higest laid off by company for every year
					
					with company_by_laids as (	
					select company, cast(format(date, 'yyyy') as int) as years, 
					sum(total_laid_off) as total_laid_off from layoff_staging2
					where format(date, 'yyyy') is not null
					group by company, format(date, 'yyyy')
					) 

					select *, DENSE_RANK() over (partition by years order by total_laid_off desc) as ranks from company_by_laids 
					order by ranks asc, years asc
					
					-- 2020 7525, 2021 3600 , 2022 11000

					select company, format(date, 'yyyy') as years, sum(total_laid_off) as total_laid_off from layoff_staging2
					group by company, format(date, 'yyyy')
					order by total_laid_off desc, years asc


					-- Find the 3 higest laid off by companies for every year
					
					with company_by_laids as (	
					select company, cast(format(date, 'yyyy') as int) as years, 
					sum(total_laid_off) as total_laid_off from layoff_staging2
					where format(date, 'yyyy') is not null
					group by company, format(date, 'yyyy')
					),
					ranking_table as
					(
					select *, DENSE_RANK() over (partition by years order by total_laid_off desc) as ranks from company_by_laids 
					)
					select * from ranking_table where ranks<=5
					