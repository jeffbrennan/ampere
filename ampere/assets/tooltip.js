window.dccFunctions = window.dccFunctions || {};
window.dccFunctions.secondsToYMD = function(value) {
    const date = new Date(value * 1000);
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
}
