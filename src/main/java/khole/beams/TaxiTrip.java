/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package khole.beams;

import java.io.Serializable;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import com.google.gson.annotations.SerializedName;

@DefaultCoder(AvroCoder.class)
public class TaxiTrip implements Serializable {
   private static final long serialVersionUID = 1L;
   @SerializedName("Uuid")
   @Nullable
   private String uuid;
   @SerializedName("Pickup datetime")
   @Nullable 
   private String pickupDatetime;
   @SerializedName("Dropoff datetime")
   @Nullable
   private String dropoffDatetime;
   @SerializedName("Store and fwd flag")
   @Nullable
   private String storeAndFwdFlag;
   @SerializedName("Rate code")
   @Nullable
   private String rateCode;
   @SerializedName("Pickup longitude")
   @Nullable
   private Double pickupLongitude;
   @SerializedName("Pickup latitude")
   @Nullable
   private Double pickupLatitude;
   @SerializedName("Dropoff longitude")
   @Nullable
   private Double dropoffLongitude;
   @SerializedName("Dropoff latitude")
   @Nullable
   private Double dropoffLatitude;
   @SerializedName("Passenger count")
   @Nullable
   private Integer passengerCount;
   @SerializedName("Fare amount")
   @Nullable
   private Double fareAmount;
   @SerializedName("Tip amount")
   @Nullable
   private Double tipAmount;
   @SerializedName("Tolls amount")
   @Nullable
   private Double tollsAmount;
   @SerializedName("Total amount")
   @Nullable
   private Double totalAmount;
   @SerializedName("Payment type")
   @Nullable
   private String paymentType;
   @SerializedName("Distance between service")
   @Nullable
   private Integer distanceBetweenService;
   @SerializedName("Time between service")
   @Nullable
   private Integer timeBetweenService;
   @SerializedName("Trip type")
   @Nullable
   private String tripType;
   
   public String getUuid()
   {
      return uuid;
   }
   public void setUuid(String uuid)
   {
      this.uuid = uuid;
   }
   public String getPickupDatetime()
   {
      return pickupDatetime;
   }
   public void setPickupDatetime(String pickupDatetime)
   {
      this.pickupDatetime = pickupDatetime;
   }
   public String getDropoffDatetime()
   {
      return dropoffDatetime;
   }
   public void setDropoffDatetime(String dropoffDatetime)
   {
      this.dropoffDatetime = dropoffDatetime;
   }
   public String getStoreAndFwdFlag()
   {
      return storeAndFwdFlag;
   }
   public void setStoreAndFwdFlag(String storeAndFwdFlag)
   {
      this.storeAndFwdFlag = storeAndFwdFlag;
   }
   public String getRateCode()
   {
      return rateCode;
   }
   public void setRateCode(String rateCode)
   {
      this.rateCode = rateCode;
   }
   public Double getPickupLongitude()
   {
      return pickupLongitude;
   }
   public void setPickupLongitude(Double pickupLongitude)
   {
      this.pickupLongitude = pickupLongitude;
   }
   public Double getPickupLatitude()
   {
      return pickupLatitude;
   }
   public void setPickupLatitude(Double pickupLatitude)
   {
      this.pickupLatitude = pickupLatitude;
   }
   public Double getDropoffLongitude()
   {
      return dropoffLongitude;
   }
   public void setDropoffLongitude(Double dropoffLongitude)
   {
      this.dropoffLongitude = dropoffLongitude;
   }
   public Double getDropoffLatitude()
   {
      return dropoffLatitude;
   }
   public void setDropoffLatitude(Double dropoffLatitude)
   {
      this.dropoffLatitude = dropoffLatitude;
   }
   public Integer getPassengerCount()
   {
      return passengerCount;
   }
   public void setPassengerCount(Integer passengerCount)
   {
      this.passengerCount = passengerCount;
   }
   public Double getFareAmount()
   {
      return fareAmount;
   }
   public void setFareAmount(Double fareAmount)
   {
      this.fareAmount = fareAmount;
   }
   public Double getTipAmount()
   {
      return tipAmount;
   }
   public void setTipAmount(Double tipAmount)
   {
      this.tipAmount = tipAmount;
   }
   public Double getTollsAmount()
   {
      return tollsAmount;
   }
   public void setTollsAmount(Double tollsAmount)
   {
      this.tollsAmount = tollsAmount;
   }
   public Double getTotalAmount()
   {
      return totalAmount;
   }
   public void setTotalAmount(Double totalAmount)
   {
      this.totalAmount = totalAmount;
   }
   public String getPaymentType()
   {
      return paymentType;
   }
   public void setPaymentType(String paymentType)
   {
      this.paymentType = paymentType;
   }
   public Integer getDistanceBetweenService()
   {
      return distanceBetweenService;
   }
   public void setDistanceBetweenService(Integer distanceBetweenService)
   {
      this.distanceBetweenService = distanceBetweenService;
   }
   public Integer getTimeBetweenService()
   {
      return timeBetweenService;
   }
   public void setTimeBetweenService(Integer timeBetweenService)
   {
      this.timeBetweenService = timeBetweenService;
   }
   public String getTripType()
   {
      return tripType;
   }
   public void setTripType(String tripType)
   {
      this.tripType = tripType;
   }
}