window.dccFunctions = window.dccFunctions || {};
window.dccFunctions.secondsToYMD = function(value) {
    const date = new Date(value * 1000);
    const year = date.getUTCFullYear();
    const month = String(date.getUTCMonth() + 1).padStart(2, '0');
    const day = String(date.getUTCDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
}
