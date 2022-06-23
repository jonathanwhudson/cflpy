import sys
import time

def main() -> None:
    leagues.main()
    seasons.main()

if __name__ == "__main__":
    store = time.perf_counter()
    main()
    print(f"{sys.argv[0]} took {time.perf_counter() - store:.2f}s to run.")
