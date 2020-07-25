## create the plot here

set size 1,1

set boxwidth 0.5
set style fill pattern border -1

set datafile separator ","
set title "Client-Server Traffic (TPCDS-10G)"
set border 1+2
set ylabel "% of data transfered" font ",28"
set xrange [0 : 4.5]
set yrange [0 : 100]
unset xtics
## set xtics nomirror ("no-dedup" 0.7)
set ytics nomirror ("20" 20, "40" 40, "60" 60, "80" 80, "100" 100)
unset key
set key samplen 2
set key font ",28"
set key reverse right maxcolumns 1 

## plot twice, once per terminal

set terminal postscript eps enhanced color 22 font ",28"
set output "eps/orc.eps"

plot 'dat/orc/no-dedup.dat' u 1:2 with boxes title "no-dedup" fc rgb "red" fs pattern 3, \
     'dat/orc/s+p.dat' u 1:2 with boxes title "s+p" fc rgb "green" fs pattern 4, \

set terminal png size 800,600 font ',28'
set output "png/orc.png"

plot 'dat/orc/no-dedup.dat' u 1:2 with boxes title "no-dedup" fc rgb "red" fs pattern 3, \
     'dat/orc/s+p.dat' u 1:2 with boxes title "s+p" fc rgb "green" fs pattern 4, \



