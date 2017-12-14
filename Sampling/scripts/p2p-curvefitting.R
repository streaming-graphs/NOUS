data <- read.csv(file="/sumitData/work/PhD/GraphSampling/Reports/p2pdataonlys200.csv", sep=",",header = FALSE)
x<- data[,1]
y <- data[,2]
# fitting code
fitmodel <- nls(y~a/(1 + exp(-b * (x-c))), start=list(a=106,b=2000000,c=.17))
# visualization code
# get the coefficients using the coef function
params=coef(fitmodel)
newparam <- c(106,2000000,.17)
y2 <- sigmoid(newparam,x)
plot(y2,type="l")
points(y)
