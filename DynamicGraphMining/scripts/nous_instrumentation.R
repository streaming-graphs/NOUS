x <- c(1000,2000,3000,4000,5000,6000)
y <- c(301,301,303,300,298,294)
setEPS()
postscript("varying_support.eps")
plot(x,y,type="n",xlab="Support",ylab="Time (Sec)")
title("Varying Support")
lines(x,y,pch=23)
points(x,y,pch=23)
dev.off()


setEPS()
postscript("varying_workercore.eps")
x <- c(1,2,3,4,5,6)
y <- c(540,354,314,308,300,315)
plot(x,y,type="n",xlab="Worker Core",ylab="Time (Sec)")
lines(x,y,pch=23)
points(x,y,pch=23)
dev.off()


setEPS()
postscript("varying_workermem.eps")
x <- c(5,10,15,20,25)
y <- c(338,301,280,288,329)
plot(x,y,type="n",xlab="Worker Memory",ylab="Time (Sec)")
lines(x,y,pch=23)
points(x,y,pch=23)
dev.off()




setEPS()
postscript("varying_drivermem.eps")
x <- c(5,10,15,20,25,30)
y <- c(298,294,294,301,304,308)
plot(x,y,type="n",xlab="Driver Memory",ylab="Time (Sec)")
title("Varying Driver Memory")
lines(x,y,pch=23)
points(x,y,pch=23)
dev.off()


setEPS()
postscript("varying_patternsizebound.eps")
x <- c(2,4,8,16)
y <- c(224,309,371,1630)
plot(x,y,type="n",xlab="Pattern Size Bound",ylab="Time (Sec)")
title("Varying Pattern Size Bound")
lines(x,y,pch=23)
points(x,y,pch=23)
dev.off()

