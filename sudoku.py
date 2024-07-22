import time
from collections import deque
from sudoku_solver import SudokuSolver

class Sudoku:
    def __init__(self, sudoku):
        self.grid = sudoku
        self.recent_requests = deque()


    def __str__(self):
        string_representation = "| - - - - - - - - - - - |\n"

        for i in range(9):
            string_representation += "| "
            for j in range(9):
                string_representation += str(self.grid[i][j])
                string_representation += " | " if j % 3 == 2 else " "

            if i % 3 == 2:
                string_representation += "\n| - - - - - - - - - - - |"
            string_representation += "\n"

        return string_representation

    def check(self, base_delay=0.01, interval=10, threshold=5):
        """Check if the given Sudoku solution is correct.
        
        You MUST incorporate this method without modifications into your final solution.
        """

        current_time = time.time()
        self.recent_requests.append(current_time)
        num_requests = len([t for t in self.recent_requests if current_time - t < interval])

        if num_requests > threshold:
            delay = base_delay * (num_requests - threshold + 1)  # Increase delay based on excess requests
            time.sleep(delay)

        # Check rows
        for row in range(9):
            if sum(self.grid[row]) != 45 or len(set(self.grid[row])) != 9:
                return False

        # Check columns
        for col in range(9):
            if sum([self.grid[row][col] for row in range(9)]) != 45 or len(set([self.grid[row][col] for row in range(9)])) != 9:
                return False

        # Check 3x3 squares
        for i in range(3):
            for j in range(3):
                if sum([self.grid[i*3+k][j*3+l] for k in range(3) for l in range(3)]) != 45 or len(set([self.grid[i*3+k][j*3+l] for k in range(3) for l in range(3)])) != 9:
                    return False

        return True


if __name__ == "__main__":

    sudoku = Sudoku([
        [0,0,0,1,0,0,0,0,0],
        [0,0,0,3,2,0,0,0,0],
        [0,0,0,0,0,9,0,0,0],
        [0,0,0,0,0,0,0,7,0],
        [0,0,0,0,0,0,0,0,0],
        [0,0,0,9,0,0,0,0,0],
        [0,0,0,0,0,0,9,0,0],
        [0,0,0,0,0,0,0,0,3],
        [0,0,0,0,0,0,0,0,0]
    ])

    print(sudoku)
    solver = SudokuSolver(sudoku.grid)
    solver.solve()
    solved_grid = solver.grid  #solved sudoku grid

    solved_sudoku = Sudoku(solved_grid)

    if solved_sudoku.check():
        print("Sudoku is correct!")
    else:
        print("Sudoku is incorrect! Please check your solution.")

    print(solved_sudoku)
