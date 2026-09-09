# Data Model

## What the data is

Each row is a **fare quote observed on a given day** for an itinerary
(airline, flight, origin → destination, departure/arrival window, stops, cabin) at a
given booking lead-time (`days_left`). The same itinerary appears many times with
different `days_left` and often with different prices on the same day. It is therefore
modelled as a **fare-observation fact**, not as a flight schedule.

## Star schema (`marts` schema)
