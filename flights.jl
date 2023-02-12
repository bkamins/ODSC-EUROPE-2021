# # Dataframes.jl: a Perfect Sidekick for Your Next Data Science Project
# ## Bogumił Kamiński
# ### Prepared for https://odsc.com/europe/, 10:15-11:45 AM GMT, June 9, 2021

# The tutorial is loosely based on:
# https://cran.r-project.org/web/packages/data.table/vignettes/datatable-intro.html

# ## Intended usage of this tutorial

# Before you start make sure that you have
# [Julia](https://julialang.org/) installed
# and ensure that your system can find the `julia` executable.

# This updated version of the tutorial was tested under Julia 1.9.0 and DataFrames.jl 1.5.0.

# Next execute the following:
# 1. Clone GitHub repository https://github.com/bkamins/ODSC-EUROPE-2021.git
#    containing the tutorial into a local folder
# 2. Execute `julia --project` in the project folder to start Julia REPL
# 3. Press `]` to enter package manager mode and execute `instantiate`; press backspace
# 4. Follow the code contained in this file

# ## Load the packages we are going to use in this tutorial

using Arrow
using Chain
using CSV
using DataFrames
using Dates
using GLM
using HTTP
using Plots
using Statistics

# ## Basic operations on data frames

# Create a sample data frame

df = DataFrame(id = ["b","b","b","a","a","c"],
               a = 1:6, b = 7:12, c = 13:18)

#-

typeof(df)

# check its dimensions

nrow(df)

#-

ncol(df)

# You can index into a data frame just like you would index a matrix

df[1:2, 2:end] # subset rows and columns

#-

df[1, :] # get one row

#-

df[:, 1] # get one column

#-

df[1, 1] # get a cell

# The extra functionality is that:

# a) you can select columns using their names

df.id # a most common way to do the selection - treat column name as a field

#-

df[:, "id"] # column names can be passed as strings

#-

df[:, :id] # or as Symbols, which are slightly faster

# b) you can use non-standard: `Not`, `Between`, `Cols`, `All`, and `Regex` selectors

df[Not(1), Between("a", "c")]

# The indexing is much more powerful than what we have shown.
# You can find all the rules here: https://dataframes.juliadata.org/stable/lib/indexing/

# In order to get summary statistics of a data frame use the describe function

describe(df)

# ## Reading and writing CSV and Apache Arrow files

# Fetch from the Internet and load a file that we will use in this tutorial

input = "https://raw.githubusercontent.com/Rdatatable" *
        "/data.table/master/vignettes/flights14.csv"

#-

flights = CSV.read(HTTP.get(input).body, DataFrame)

# R users will likely ask if it is possible to use piping to perform
# the steps shown above. Here is how you can do it:

@chain input begin
    HTTP.get
    _.body
    CSV.read(DataFrame)
end

# by default `@chain` passes the value of the previous operation as a first
# argument to the next operation; unless you explicitly use `_` in which
# case you can put it anywhere in the expression.

# Let us inspect how `@chain` rewrites our code:

@macroexpand @chain input begin
    HTTP.get
    _.body
    CSV.read(DataFrame)
end

# The data frame

# Writing a CSV file is also easy

CSV.write("flights14.csv", flights)

# And now save the file in Apache Arrow format

Arrow.write("flights14.arrow", flights)

# Now read back the files

tmp1 = CSV.read("flights14.csv", DataFrame)

#-

tmp2 = Arrow.Table("flights14.arrow") |> DataFrame

# Check if all three tables hold the same data

flights == tmp1 == tmp2

# ## Filtering

# Assume we are only interested in flights from "EWR" to "PHL"

# We use indexing

flights[(flights.origin .== "EWR") .&& (flights.dest .== "PHL"), :]

# or specialized functions

# `filter` works row-wise

filter(row -> row.origin == "EWR" && row.dest == "PHL", flights)

# `subset` takes whole columns

subset(flights, :origin => x -> x .== "EWR", :dest => x -> x .== "PHL")

#-

subset(flights, :origin => ByRow(==("EWR")), :dest => ByRow(==("PHL")))

# you can also set an index on `:origin` and `:dest` columns for fast lookup
# if you expect to do it many times

flights_idx = groupby(flights, [:origin, :dest])

#-

flights_idx[("EWR", "PHL")]

# The detailed comparison of `filter` and `subset` design is discussed in
# the following post: https://bkamins.github.io/julialang/2021/05/07/subset.html.
# Here let me just comment that working on whole columns is useful when you
# want to define filtering conditions based on group aggregates.
# For instance for every `:origin`-`:dest` pair pick the flight that had highest `:air_time`

subset(flights_idx, :air_time => x -> x .== maximum(x))

# Why do we have more rows than `:origin`-`:dest` pairs? This question naturally
# leads us to the next section.

# ## Aggregation using split-apply-combine

combine(flights_idx) do sdf
    max_air_time = maximum(sdf.air_time)
    return count(sdf.air_time .== max_air_time)
end

# Let us keep only the cases when we have more than one entry per group

@chain flights_idx begin
    combine(_) do sdf
        max_air_time = maximum(sdf.air_time)
        return count(sdf.air_time .== max_air_time)
    end
    filter(:x1 => >(1), _)
end

# Simple aggregations are supported quite conveniently.
# For each `:month` pair find number of flights and average `:dep_delay`

@chain flights begin
    groupby(:month)
    combine(nrow, :dep_delay => mean)
end

# DataFrames.jl ensures a reasonably good performance of such aggregations.
# See https://h2oai.github.io/db-benchmark/ for a benchmark

# ## Sorting

# Assume we want to sort the data frame that we have just obtained by `:dep_delay_mean`

@chain flights begin
    groupby(:month)
    combine(nrow, :dep_delay => mean)
    sort(:dep_delay_mean)
end

# You can sort by multiple columns and can specify complex ordering rules, e.g.

sort(df, [:id, order(:a, rev=true)])

# ## Joining

# Assume we have a dictionary table with month names:

months = DataFrame(month=1:10,
                   month_name=["Jan", "Feb", "Mar", "Apr", "May",
                               "Jun", "Jul", "Aug", "Sep", "Oct"])

# We want to add the column `:month_name` to our original table

leftjoin(flights, months, on=:month)

# Let us add it to our earlier analysis

@chain flights begin
    leftjoin(months, on=:month)
    groupby(:month_name)
    combine(nrow, :dep_delay => mean)
    sort(:dep_delay_mean)
end

# All standard joins are supported: `innerjoin`, `leftjoin`, `rightjoin`,
# `outerjoin`, `semijoin`, `antijoin`, `crossjoin`, also in-place `leftjoin!`

# ## Reshaping

# Assume we want to see a cross tabulation of number of flights per carrier
# in consecutive months.
# We already know how to get this information in long format.

@chain flights begin
    groupby([:month, :carrier], sort=true)
    combine(nrow)
end

# If we wanted to get carriers as consecutive columns we can
# reshape the data frame into wide format

@chain flights begin
    groupby([:month, :carrier], sort=true)
    combine(nrow)
    unstack(:month, :carrier, :nrow)
end

# You can also do it in one shot

unstack(flights, :month, :carrier, :carrier, combine=length)

# In order to go from wide to long format use the `stack` function.

# ## Transforming

# We already know the combine function which combines rows by aggregating them.
# DataFrames.jl also provides `select` and `transform` functions that always
# retain the number and order of rows when doing transformations

# Assume we wanted to add a `:total_delay` column to our data frame that
# is a sum of `:dep_delay` and `:arr_delay`.

# The simplest way to do it is just

flights.total_delay = flights.dep_delay + flights.arr_delay

#-

flights

# A declarative way to do the same is (note `!` which signals in-place operation)

transform!(flights, [:dep_delay, :arr_delay] => (+) => :total_delay)

# The general syntax of all transformations in DataFrames.jl is
# `source_columns => transformation_function => target_column_names`

# Let us make one more example: convert `:year`, `:month`, and `:day` columns into a date:

select(flights, [:year, :month, :day], [:year, :month, :day] => ByRow(Date))

# Note that `select`, as opposed to `transform`, only keeps the columns that are specified.
# Also observe that DataFrames.jl tries to auto-generate a meaningful target column
# name in case it is not passed by the user.

# ## Integration with the ecosystem: plotting data, building basic predictive models

# Before concluding the tutorial let us build a linear model and plot explaining
# average `:total_delay` by `:month`

# Note that `@aside` inside `@chain` allows us to fork the execution of the pipe.

@chain flights begin
    groupby(:month, sort=true)
    combine(:total_delay => mean => :mean_delay)
    @aside lm(@formula(mean_delay~month), _) |> display
    plot(_.month, _.mean_delay, label=nothing,
         xlabel="month", ylabel="mean delay")
end

# ## Why do I call DataFrames.jl a sidekick?

# Consider the following data frame (example supplied by Nils Gudat):

df_long = DataFrame(id=1:10^7,
                    trials=repeat(1:10, 10^6),
                    heads=repeat(1:2, 5*10^6))

# We want to expand this table to hold `:id` and `:toss` columns
# where `:toss` is a `Bool` variable indicating if head of tail was tossed
# You can do it e.g. like this

function expand(t, h)
    @assert 0 <= h <= t
    x = falses(t)
    x[1:h] .= true
    return x
end

@chain df_long begin
    combine(:id, [:trials, :heads] => ByRow(expand) => :toss)
    @aside println(first(_, 5))
    flatten(:toss)
end

# However, it is quite slow

@time @chain df_long begin
    combine(:id, [:trials, :heads] => ByRow(expand) => :toss)
    flatten(:toss)
end;

# The beauty of Julia is in that case you can write
# a user defined function that will be fast and fully generic

function fast_expand(id, t, h)
    @assert length(id) == length(t) == length(h)
    @assert all(x -> 0 <= x[1] <= x[2], zip(h, t))
    st = sum(t)
    x = falses(st)
    idx = similar(id, st)
    i = 1
    for (idv, nt, ns) in zip(id, t, h)
       x[i:i+ns-1] .= true
       idx[i:i+nt-1] .= Ref(idv)
       i += nt
    end
    return DataFrame("id" => idx, "toss" => x, copycols=false)
end

# additionally we can learn to use the `eachcol` function

fast_expand(eachcol(df_long)...)

# let us check if the results match

fast_expand(eachcol(df_long)...) == @chain df_long begin
    combine(:id, [:trials, :heads] => ByRow(expand) => :toss)
    flatten(:toss)
end

# and the timing

@time fast_expand(eachcol(df_long)...);

# ## Concluding remarks

# Before I finish let me mention two packages.

# DataFramesMeta.jl: https://github.com/JuliaData/DataFramesMeta.jl
# is a very nice package that allows one to use R-like references to variable
# names in expressions

# Literate.jl can be used to convert this file into Jupyter Notebook.
# Just run the following commands in your OS prompt to see the result:
# ```
# julia --project -e "using Literate; Literate.notebook(\"flights.jl\", execute=false)"
# julia --project -e "using IJulia; notebook(dir=pwd())"
# ```
