# HydroVu test plan

Date: August 11, 2025
Status: Draft, unstarted

## Background

We will construct a program to run a long-running correctness test of
the HydroVu integration.

Our goal is to ensure that the integration so far prototyped in the
crates/hydrovu main program, can collect full data sets reliably.

Each HydroVu instrument, identified locationID parameter, consists of
a timeseries that we collect in forward order staring from UNIX
timestamp 0 (i.e., the epoch). 

As we know, these instruments have a history of schema change, which
our system is designed to work with. When we complete this testing, we
will have verified all the instruments that we have access to can be
read in full.

Most of the instruments we began span a time period beginning after
Febuary 2024 and continuing through the present. Some instruments were
removed from service and some are still active.

We will use the existing configuration struct, which accepts a list of
instruments. 

## Objective

Create a new main program which accepts the existing configuration.

It will create a new pond instance as by running "pond init"
(referring to the main program in crates/cmd) and create the necessary
directories. 

Then, it will execute a series of transactions. Each transaction will
collect the next available `max_rows_per_run` points for each
instrument, until each instrument reaches the end of the series, which
is when 0 points are retrieved after the youngest timestamp that was
collected for the instrument.

We commit these transactions to avoid any one transaction becoming too
large. In pseudocode, we should:

1. Initialize new pond.
2. Until all instruments are fully collected, repeat:
  - begin transaction
  - for each instrument, get last timestamp, request new data, write new data
  - commit transaction

When the test reaches this point, save the final-collection timestamp
for each instrument. We are going to verify that the data we have written
to the FileSeries corresponding with each instrument matches the data
that we read in a second pass.

In the second pass, we will:

1. For each instrument:
  - open the FileSeries with temporal range set to [0, youngest-collected-timestamp]
  - receive points in order ascending by timestamp, verify exact match for all columns

After this process, we will be confidence in our HydroVu data.

  
