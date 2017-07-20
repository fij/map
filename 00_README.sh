#!/bin/sh                                                                                                                                         

# A small crawler that maps the hyperlinks within a given domain. Example: jnj.com (Johnson & Johnson).

nohup nice -n 19 \                                                                                                                                
      python2.7 \                                                                                                                                 
      01_crawler.py \                                                                                                                             
      "http://www.jnj.com" \                                                                                                                      
      60 \                                                                                                                                        
      5 \                                                                                                                                         
      2 \                                                                                                                                         
      "jnj.com" \                                                                                                                                 
      02_nodes.txt \                                                                                                                              
      03_links.txt \                                                                                                                              
      1 \                                                                                                                                         
      1 \                                                                                                                                         
      >04_stdout.txt \                                                                                                                            
      2>05_stderr.txt \                                                                                                                           
&
