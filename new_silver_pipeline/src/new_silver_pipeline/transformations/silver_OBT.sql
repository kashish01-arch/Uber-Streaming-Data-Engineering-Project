CREATE OR REFRESH MATERIALIZED VIEW silver_obt
AS
SELECT
  
    stg_rides_dedup.ride_id,stg_rides_dedup.confirmation_number,stg_rides_dedup.passenger_id,stg_rides_dedup.driver_id,stg_rides_dedup.vehicle_id,stg_rides_dedup.pickup_location_id,stg_rides_dedup.dropoff_location_id,stg_rides_dedup.vehicle_type_id,stg_rides_dedup.vehicle_make_id,stg_rides_dedup.payment_method_id,stg_rides_dedup.ride_status_id,stg_rides_dedup.pickup_city_id,stg_rides_dedup.dropoff_city_id,stg_rides_dedup.cancellation_reason_id,stg_rides_dedup.passenger_name,stg_rides_dedup.passenger_email,stg_rides_dedup.passenger_phone,stg_rides_dedup.driver_name,stg_rides_dedup.driver_rating,stg_rides_dedup.driver_phone,stg_rides_dedup.driver_license,stg_rides_dedup.vehicle_model,stg_rides_dedup.vehicle_color,stg_rides_dedup.license_plate,stg_rides_dedup.pickup_address,stg_rides_dedup.pickup_latitude,stg_rides_dedup.pickup_longitude,stg_rides_dedup.dropoff_address,stg_rides_dedup.dropoff_latitude,stg_rides_dedup.dropoff_longitude,stg_rides_dedup.distance_miles,stg_rides_dedup.duration_minutes,stg_rides_dedup.booking_timestamp,stg_rides_dedup.pickup_timestamp,stg_rides_dedup.dropoff_timestamp,stg_rides_dedup.base_fare,stg_rides_dedup.distance_fare,stg_rides_dedup.time_fare,stg_rides_dedup.surge_multiplier,stg_rides_dedup.subtotal,stg_rides_dedup.tip_amount,stg_rides_dedup.total_fare,stg_rides_dedup.rating,
        
  
    map_vehicle_makes.vehicle_make
        
        ,
        
  
    map_vehicle_types.vehicle_type, description, base_rate, per_mile, per_minute
        
        ,
        
  
    uber_catalog.uber_schema.map_ride_statuses.ride_status
        
        ,
        
  
    uber_catalog.uber_schema.map_payment_methods.payment_method,map_payment_methods.is_card,map_payment_methods.requires_auth
        
        ,
        
  
    uber_catalog.uber_schema.map_cities.city as pickup_city,state,region
        
        ,
        
  
    uber_catalog.uber_schema.map_cancellation_reasons.cancellation_reason
        
  

FROM
  
    
        uber_catalog.uber_schema.stg_rides_dedup AS stg_rides_dedup
    
  
    
        LEFT JOIN uber_catalog.uber_schema.map_vehicle_makes ON stg_rides_dedup.vehicle_make_id = uber_catalog.uber_schema.map_vehicle_makes.vehicle_make_id
    
  
    
        LEFT JOIN uber_catalog.uber_schema.map_vehicle_types ON stg_rides_dedup.vehicle_type_id = uber_catalog.uber_schema.map_vehicle_types.vehicle_type_id
    
  
    
        LEFT JOIN uber_catalog.uber_schema.map_ride_statuses ON stg_rides_dedup.ride_status_id = uber_catalog.uber_schema.map_ride_statuses.ride_status_id
    
  
    
        LEFT JOIN uber_catalog.uber_schema.map_payment_methods ON stg_rides_dedup.payment_method_id = uber_catalog.uber_schema.map_payment_methods.payment_method_id
    
  
    
        LEFT JOIN uber_catalog.uber_schema.map_cities ON stg_rides_dedup.pickup_city_id = uber_catalog.uber_schema.map_cities.city_id
    
    
        
        LEFT JOIN uber_catalog.uber_schema.map_cancellation_reasons ON stg_rides_dedup.cancellation_reason_id = uber_catalog.uber_schema.map_cancellation_reasons.cancellation_reason_id
    