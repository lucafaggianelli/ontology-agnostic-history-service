from smart_m3.m3_kp_api import *
m3 = m3_kp_api()

ns = 'http://rdf.tesladocet.com/ns/person-car.owl#'

ins = [
Triple( URI(ns+'Person_1'), URI(ns+'HasCar'), URI(ns+'Car_1')),
Triple( URI(ns+'Car_1'), URI(ns+'HasBrand'), URI(ns+'Brand_1')),
Triple( URI(ns+'Car_1'), URI(ns+'HasTire'), URI(ns+'Tire_1')),
Triple( URI(ns+'Tire_1'), URI(ns+'HasTireTread'), URI(ns+'TireTread_1')),

Triple( URI(ns+'Car_2'), URI(ns+'HasBrand'), URI(ns+'Brand_2')),

Triple( URI(ns+'Car_3'), URI(ns+'HasTire'), URI(ns+'Tire_3')),
Triple( URI(ns+'Tire_3'), URI(ns+'HasTireTread'), URI(ns+'TireTread_3')),

Triple( URI(ns+'Car_4'), URI(ns+'HasBrand'), URI(ns+'Brand_4')),
Triple( URI(ns+'Car_4'), URI(ns+'HasTire'), URI(ns+'Tire_4')),

Triple( URI(ns+'Person_5'), URI(ns+'HasCar'), URI(ns+'Car_5'))
]