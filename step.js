/**
 * Created by andriy on 22.12.16.
 */
"use strict";

function* genStrategy(n) {
    // here is working algorithm
    for (let i = 0; i < n; i++) {
        let time = new Date();
        console.log("step " + time.getTime());
        yield 1000; // making pause 1000 ms
    }
    console.log("finish");
    return null;
}

let step = genStrategy(10);
let state = null;
let stepCallback = function(){
    state = step.next();
    if (!state.done) {
        setTimeout(stepCallback, state.value);
    }
};
stepCallback();
