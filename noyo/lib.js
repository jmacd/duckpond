import * as Inputs from "npm:@observablehq/inputs";

export const timelist = [
  ["1 Week", 7],
  ["2 Weeks", 14],
  ["1 Month", 30],
  ["2 Months", 60],
  ["3 Months", 90],
  ["6 Months", 180],
  ["12 Months", 365],
  ["18 Months", 550],
];

export async function timepicker() {
  return Inputs.radio(
    new Map(timelist),
    {
                value: 30, 
                label: "Time range", 
    }
  )
}

export function timerange(pick) {
    var res;
    if (pick <= 30) {
        res = "1h";
    } else if (pick <= 60) {
        res = "2h";
    } else if (pick <= 90) {
        res = "4h";
    } else if (pick <= 180) {
        res = "12h";
    } else {
        res = "24h";
    }
    console.log("RES", res)
    return res;
}

export function legendName(i, n, u) {
    // Note: n = name, is in the plot title
    //       u = unit, is on the y-axis
    //       i = instrument, this has <location>.<model> and we only use location
    return i.split(".").join(" ")
}

export function plotName(n) {
    if (n == "DO") {
	return "Disolved Oxygen";
    }
    return n;
}
