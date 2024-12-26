CREATE OR REPLACE TABLE `product_recommendation_details_latest_v2`
(
  name STRING,
  id STRING NOT NULL,
  type STRING,
  primaryProductId STRING,
  -- collectionMemberIds ARRAY<STRING>,
  -- gtin STRING,
  categories ARRAY<STRING>,
  title STRING NOT NULL,
  brands ARRAY<STRING>,
  description STRING,
  languageCode STRING,
  -- attributes ARRAY<STRUCT<key STRING, value STRUCT<text ARRAY<STRING>, numbers ARRAY<FLOAT64>>>>,
  tags ARRAY<STRING>,
  priceInfo STRUCT<currencyCode STRING, price FLOAT64, originalPrice FLOAT64, cost FLOAT64, priceEffectiveTime STRING, priceExpireTime STRING>,
  rating STRUCT<ratingCount INT64, averageRating FLOAT64, ratingHistogram ARRAY<INT64>>,
  expireTime STRING,
  availableTime STRING,
  availability STRING,
  availableQuantity INT64,
  fulfillmentInfo ARRAY<STRUCT<type STRING, placeIds ARRAY<STRING>>>,
  uri STRING,
  images ARRAY<STRUCT<uri STRING NOT NULL, height INT64, width INT64>>,
  audience STRUCT<genders ARRAY<STRING>, ageGroups ARRAY<STRING>>,
  -- colorInfo STRUCT<colorFamilies ARRAY<STRING>, colors ARRAY<STRING>>,
  -- sizes ARRAY<STRING>,
  -- materials ARRAY<STRING>,
  -- patterns ARRAY<STRING>,
  conditions ARRAY<STRING>,
  -- retrievableFields STRING,
  publishTime STRING,
  -- promotions ARRAY<STRUCT<promotionId STRING>>
) as 

WITH
  dim_verified_sku as (
    SELECT
      date_key,
      product_name,
      sku,
    FROM 
      `google_recommendation_product_verified`
  ),
  product_rating_data as (
    SELECT
      date_key,
      product_id,
      ratingCount,
      master_product_rating,
      ratingHistogram,
      _1_start,
      _2_start,
      _3_start,
      _4_start,
      _5_start
    FROM 
      `google_recommendation_product_rating`
  ),
  full_log_data as (
    SELECT
      CAST(COALESCE(a.product_name,b.product_name) as STRING) as name,
      CAST(IFNULL(b.product_key,0) as STRING) as id,
      'PRIMARY' as type,
      '' as primaryProductId,
      ARRAY[IFNULL(b.primary_cate,'')] as categories,
      CAST(IFNULL(COALESCE(a.product_name,b.product_name),'test') as STRING) as title,
      ARRAY[IFNULL(b.brand,'OEM')] as brands,
      CAST(IFNULL(COALESCE(a.product_name,b.product_name),'test') as STRING) as description,
      'vi' as languageCode,
      ARRAY_CONCAT(
        [ifnull(origin,"empty")],
        [ifnull(primary_cate,"empty")] 
      ) as tags,
      STRUCT(
        'VND' as currencyCode, 
        CAST(IFNULL(sale_price,0) as FLOAT64) as price, 
        SAFE_CAST(
          CASE WHEN IFNULL(list_price,0) < IFNULL(sale_price,0) THEN IFNULL(sale_price,0) 
          ELSE IFNULL(list_price,0) 
        END as FLOAT64) as originalPrice,
        CAST(IFNULL(sale_price,0) as FLOAT64) as cost, 
        CONCAT(FORMAT_DATETIME('%Y-%m-%d',DATE_SUB(CURRENT_DATETIME('+7'),INTERVAL 1 YEAR)),'T',FORMAT_DATETIME('%H:%M:%S',DATE_SUB(CURRENT_DATETIME('+7'),INTERVAL 1 YEAR)),'Z') as priceEffectiveTime, 
        CONCAT(FORMAT_DATETIME('%Y-%m-%d',DATE_ADD(CURRENT_DATETIME('+7'),INTERVAL 5 YEAR)),'T',FORMAT_DATETIME('%H:%M:%S',DATE_ADD(CURRENT_DATETIME('+7'),INTERVAL 5 YEAR)),'Z') as priceExpireTime
      ) as priceInfo,
      STRUCT(
        SAFE_CAST(IFNULL(d.ratingCount,1) as INT64) as ratingCount, 
        CAST(IFNULL(d.master_product_rating,5) as FLOAT64) as averageRating,
        CASE
          WHEN d.ratingHistogram IS NULL THEN ARRAY[0,0,0,0,1]
          ELSE ARRAY[IFNULL(d._1_start,0),IFNULL(d._2_start,0),IFNULL(d._3_start,0),IFNULL(d._4_start,0),IFNULL(d._5_start,0)]
        END as ratingHistogram
      ) as rating,
      CONCAT(FORMAT_DATETIME('%Y-%m-%d',DATE_ADD(CURRENT_DATETIME('+7'),INTERVAL 5 YEAR)),'T',FORMAT_DATETIME('%H:%M:%S',DATE_ADD(CURRENT_DATETIME('+7'),INTERVAL 5 YEAR)),'Z') as expireTime,
      CONCAT(FORMAT_DATETIME('%Y-%m-%d',DATE_SUB(CURRENT_DATETIME('+7'),INTERVAL 1 YEAR)),'T',FORMAT_DATETIME('%H:%M:%S',DATE_SUB(CURRENT_DATETIME('+7'),INTERVAL 1 YEAR)),'Z') as  availableTime,
      'IN_STOCK' as availability,
      SAFE_CAST((CASE 
                  WHEN b.qty_available < 0 THEN 0 
                  WHEN b.qty_available IS NULL THEN 0 
                  ELSE b.qty_available 
                END) as INT64) as  availableQuantity,
      [STRUCT(
        'custom-type-1' as type, 
        ARRAY[CAST(b.seller_id as STRING)] as placeIds
      )] as fulfillmentInfo,
      IFNULL(CONCAT('https://tiki.vn/',b.url_path),'') as uri,
      ARRAY[
        STRUCT(
          IFNULL(CASE
                    WHEN LOWER(CAST(REGEXP_CONTAINS(thumbnail, r'^http.+') as STRING)) = 'true' THEN thumbnail
                    WHEN LOWER(CAST(REGEXP_CONTAINS(thumbnail, r'^\/.+') as STRING)) = 'true' THEN CONCAT('https://salt.tikicdn.com/cache/750x750/media/catalog/product',thumbnail)
                    WHEN LOWER(CAST(REGEXP_CONTAINS(thumbnail, r'^\".+') as STRING)) = 'true' THEN CONCAT('https://salt.tikicdn.com/cache/750x750/media/catalog/product',REGEXP_EXTRACT(thumbnail, r'^\"(.+)\"'))
                    ELSE CONCAT('https://salt.tikicdn.com/cache/750x750/media/catalog/product/',thumbnail)
                  END,'') as uri,
          750 as height, 
          750 as width
      )] as images,
      STRUCT(
        CASE 
          WHEN cate2 LIKE '%nam%' THEN  ARRAY['female','unisex']
          WHEN cate2 LIKE '%nữ%' OR cate2 LIKE '%Mẹ%' THEN  ARRAY['male','unisex']
          ELSE ARRAY['male','female','unisex']
        END as genders,
        CASE 
          WHEN cate2 LIKE '%trẻ em%' OR cate2 LIKE '%cho bé%' OR cate2 LIKE '%Tã, Bỉm%' THEN  ARRAY['kids']
          WHEN cate2 LIKE '%cho người lớn%' OR cate2 LIKE '%Hỗ trợ tình dục%' OR cate2 LIKE '%có cồn%'THEN ARRAY['adult']
          ELSE ARRAY['kids', 'adult']
        END as ageGroups
      ) as audience,
      ARRAY['new'] as conditions,
      CAST(
        CASE
          WHEN b.publication_date IS NULL THEN CONCAT(FORMAT_DATETIME('%Y-%m-%d',DATE_SUB(CURRENT_DATETIME('+7'),INTERVAL 1 YEAR)),'T',FORMAT_DATETIME('%H:%M:%S',DATE_SUB(CURRENT_DATETIME('+7'),INTERVAL 1 YEAR)),'Z')
          ELSE CONCAT(FORMAT_DATETIME('%Y-%m-%d',DATETIME(b.publication_date)),'T',FORMAT_DATETIME('%H:%M:%S',DATETIME(b.publication_date)),'Z')
      END as STRING) as publishTime,
      thumbnail,
      SAFE_CAST(
        CASE WHEN IFNULL(list_price,0) < IFNULL(sale_price,0) THEN IFNULL(sale_price,0) 
        ELSE IFNULL(list_price,0) 
      END as FLOAT64) as originalPrice
    FROM 
      dim_verified_sku a
    LEFT JOIN 
      `dim_product_full` b
    ON
      a.sku = b.sku
    LEFT JOIN 
      `seo_table_sitemap_product_master_meta_description` c
    ON b.product_key = c.product_id
    LEFT JOIN
      product_rating_data d
    ON b.product_key = d.product_id 
  )
  SELECT
    * EXCEPT(thumbnail,originalPrice)
  FROM 
    full_log_data
  WHERE 1=1
    AND thumbnail IS NOT NULL
    AND originalPrice != 0

