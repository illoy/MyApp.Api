using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using MyApp.Data.Context;
using MyApp.Data.Data;
using MyApp.Data.DTO;

namespace MyApp.Api.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class UsersControllers : ControllerBase
    {
        private readonly AppDbContext _context;
        private readonly KafkaProducer _kafka;
        public UsersControllers(AppDbContext context, KafkaProducer kafka)
        {
            _context = context;
            _kafka = kafka;
        }

        [HttpGet("ShowAllUsers")]
        public async Task<IActionResult> GetUsers()
        {
            var users = await _context.Users.ToListAsync();
            return Ok(users);
        }

        [HttpPost("AddUser")]
        public async Task<IActionResult> CreateUser([FromBody] User user)
        {
            if (user == null || string.IsNullOrEmpty(user.Name))
            {
                return BadRequest("Invalid user data.");
            }
            _context.Users.Add(user);

            await _context.SaveChangesAsync();
            var message = $"Create User {user.Name} with balance: {user.Balance}$";
            await _kafka.SendMessageAsync(message);

            return CreatedAtAction(nameof(GetUsers), new { id = user.Id }, user);
        }

        [HttpPost("TransferRequest")]
        public async Task<IActionResult> UpdateUserBalance([FromBody] TransferRequest req)
        {
            var fromUser = await _context.Users.FindAsync(req.FromUserId);
            var toUser = await _context.Users.FindAsync(req.ToUserId);

            if (fromUser == null || toUser == null)
            {
                return NotFound("One or both users not found.");
            }
            if(fromUser.Balance < req.Amount)
            {
                return BadRequest("Insufficient balance.");
            }

            fromUser.Balance -= req.Amount;
            toUser.Balance += req.Amount;

            await _context.SaveChangesAsync();
            var message = $"Transferred {req.Amount}$ from {fromUser.Name} to {toUser.Name}";
            await _kafka.SendMessageAsync(message);

            return Ok(new { Message = "Gooood transaction", From = fromUser, Tp = toUser});
        }
    }
}
