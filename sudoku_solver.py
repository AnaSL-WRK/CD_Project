class SudokuSolver:
    def __init__(self):
        self.validations = 0

    def is_valid(self, board, row, col, num):
        self.validations += 1

        for x in range(9):
            if board[row][x] == num or board[x][col] == num:
                return False

        start_row, start_col = 3 * (row // 3), 3 * (col // 3)

        for i in range(3):
            for j in range(3):
                if board[start_row + i][start_col + j] == num:
                    return False

        return True

    def solve(self, board):
        empty = self.find_empty_location(board)

        if not empty:
            return True
        row, col = empty

        for num in range(1, 10):
            if self.is_valid(board, row, col, num):
                board[row][col] = num

                if self.solve(board):
                    return True
                board[row][col] = 0

        return False

    def find_empty_location(self, board):
        for i in range(9):
            for j in range(9):
                if board[i][j] == 0:
                    return (i, j)
        return None

    def get_subgrid(self, board, subgrid_num):
        row_start = (subgrid_num // 3) * 3
        col_start = (subgrid_num % 3) * 3
        return [board[row_start + i][col_start + j] for i in range(3) for j in range(3)]

    def set_subgrid(self, board, subgrid_num, subgrid):
        row_start = (subgrid_num // 3) * 3
        col_start = (subgrid_num % 3) * 3
        for i in range(3):
            for j in range(3):
                board[row_start + i][col_start + j] = subgrid[i * 3 + j]
