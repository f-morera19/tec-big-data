-- Initialize the database. Create "flights" table.
BEGIN;

CREATE TABLE flights (
  age integer NOT NULL,
  light_distance integer NOT NULL,
  seat_comfort integer NOT NULL,
  departure_arrival_time_convenient integer NOT NULL,
  food_drink integer NOT NULL,
  gate_location integer NOT NULL,
  inflight_wifi_service integer NOT NULL,
  inflight_entertainment integer NOT NULL,
  online_support integer NOT NULL,
  ease_of_online_booking integer NOT NULL,
  onboard_service integer NOT NULL,
  leg_room_service integer NOT NULL,
  baggage_handling integer NOT NULL,
  checkin_service integer NOT NULL,
  cleanliness integer NOT NULL,
  online_boarding integer NOT NULL,
  departure_delay_minutes integer NOT NULL,
  arrival_delay_minutes integer NOT NULL,
  isSatisfied integer NOT NULL,
  isMale integer NOT NULL,
  isFemale integer NOT NULL,
  isLoyalCustomer integer NOT NULL,
  isDisloyalCustomer integer NOT NULL,
  isBusinessTravel integer NOT NULL,
  isPersonalTravel integer NOT NULL,
  isBusinessClass integer NOT NULL,
  isEcoClass integer NOT NULL,
  isOtherClass integer NOT NULL
);

COMMIT;