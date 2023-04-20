document.addEventListener("DOMContentLoaded", function () {
  const car = document.getElementById("car");
  const game = document.getElementById("game");

  let movingRight = true;
  let carPosition = 0;

  function moveCar() {
    if (movingRight) {
      carPosition += 5;
      if (carPosition >= game.clientWidth - car.clientWidth) {
        movingRight = false;
        car.style.transform = "scaleX(1)";
      }
    } else {
      carPosition -= 5;
      if (carPosition <= 0) {
        movingRight = true;
        car.style.transform = "scaleX(-1)";
      }
    }
    car.style.left = carPosition + "px";
  }

  setInterval(moveCar, 100);
});
