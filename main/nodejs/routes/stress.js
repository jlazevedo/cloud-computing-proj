/*
	CPU Stress Simulation
*/
var count = 0;
exports.doStress = function fibonacci(n) {
	if(count > 999999999)
		return 1;
  if (n < 2)
    return 1;
  else {
  	count++;
    return fibonacci(n-2) + fibonacci(n-1);
   }
};